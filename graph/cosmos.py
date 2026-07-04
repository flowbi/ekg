"""
graph/cosmos.py  –  Azure Cosmos DB for Apache Gremlin target.

Incremental writes  →  Gremlin WebSocket via gremlin-python
Bulk load           →  Batched Gremlin upserts (Cosmos DB has no native
                        bulk-import API for the Gremlin surface; the Graph
                        Bulk Executor library is .NET/Java only).  Large
                        initial loads should be done via the Azure Data
                        Factory Cosmos DB connector or azure-cosmosdb-bulk-executor.

Required options (GraphTargetConfig.options)
────────────────────────────────────────────
  endpoint        str   Gremlin endpoint, e.g.
                        "wss://my-account.gremlin.cosmos.azure.com:443/"
  username        str   "/dbs/<db>/colls/<graph>" (resolved via secrets)
  password        str   Primary key (resolved via secrets)
  database        str   Cosmos DB database name
  graph           str   Cosmos DB graph / container name
  bulk_batch      int   Upserts per batch during bulk load (default 100)
  traversal_src   str   Override traversal source name (default "g")

Dependencies
────────────
  pip install gremlinpython
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Optional

from graph.base import BulkRecord, GraphTarget, GraphTargetConfig

log = logging.getLogger("ekg_etl.graph.cosmos")


def _esc(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "\\'")


def _gval(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return f"'{_esc(str(value))}'"


def _prop_chain(props: Dict[str, Any]) -> str:
    return "".join(f".property('{_esc(k)}', {_gval(v)})" for k, v in props.items())


class CosmosGraphTarget(GraphTarget):
    """
    Azure Cosmos DB for Apache Gremlin implementation.

    Upsert pattern uses coalesce(unfold, addV/addE) for idempotency.
    Cosmos DB Gremlin supports only single-cardinality properties on
    edges; node properties use single() cardinality explicitly.
    The partition key property must be included in props for nodes in
    partitioned graphs — this is the caller's responsibility via
    attribute_mapping.
    """

    def __init__(self, config: GraphTargetConfig) -> None:
        super().__init__(config)
        try:
            from gremlin_python.driver import client as gremlin_client  # type: ignore
            from gremlin_python.driver import serializer             # type: ignore
        except ImportError as exc:
            raise ImportError("pip install gremlinpython") from exc

        endpoint   = config.get("endpoint", "")
        username   = config.get("username", "")
        password   = config.get("password", "")
        self._bulk_batch = config.get("bulk_batch", 100)
        self._g          = config.get("traversal_src", "g")

        self._client = gremlin_client.Client(
            endpoint,
            "g",
            username=username,
            password=password,
            message_serializer=serializer.GraphSONSerializersV2d0(),
        )
        log.info("Cosmos DB Gremlin target connected to %s", endpoint)

        # Bulk buffers
        self._bulk_nodes: List[BulkRecord] = []
        self._bulk_edges: List[BulkRecord] = []

    # ------------------------------------------------------------------
    # Internal Gremlin execution
    # ------------------------------------------------------------------

    def _submit(self, query: str) -> None:
        callback = self._client.submitAsync(query)
        result = callback.result()
        if result.status_attributes.get("x-ms-status-code", 200) >= 400:
            raise RuntimeError(f"Cosmos Gremlin error: {result.status_attributes}")

    # ------------------------------------------------------------------
    # Incremental operations
    # ------------------------------------------------------------------

    def upsert_node(self, node_id: str, label: str, props: Dict[str, Any]) -> None:
        self._submit(
            f"{self._g}.V('{_esc(node_id)}').fold().coalesce("
            f"__.unfold(),"
            f"__.addV('{_esc(label)}').property('id','{_esc(node_id)}')"
            f"){_prop_chain(props)}"
        )

    def upsert_edge(
        self, edge_id: str, label: str, from_id: str, to_id: str, props: Dict[str, Any]
    ) -> None:
        self._submit(
            f"{self._g}.E('{_esc(edge_id)}').fold().coalesce("
            f"__.unfold(),"
            f"{self._g}.V('{_esc(from_id)}')"
            f".addE('{_esc(label)}')"
            f".to({self._g}.V('{_esc(to_id)}'))"
            f".property('id','{_esc(edge_id)}')"
            f"){_prop_chain(props)}"
        )

    def delete_vertex(self, node_id: str) -> None:
        self._submit(f"{self._g}.V('{_esc(node_id)}').drop()")

    def delete_edge(self, edge_id: str) -> None:
        self._submit(f"{self._g}.E('{_esc(edge_id)}').drop()")

    def upsert_concept_tag(
        self, tag_id: str, tag_name: str, tag_category: str, display_name: Optional[str]
    ) -> None:
        dn = display_name or tag_name
        self._submit(
            f"{self._g}.V('{_esc(tag_id)}').fold().coalesce("
            f"__.unfold(),"
            f"__.addV('ConceptTag').property('id','{_esc(tag_id)}')"
            f").property('name','{_esc(tag_name)}')"
            f".property('category','{_esc(tag_category)}')"
            f".property('display_name','{_esc(dn)}')"
        )

    def upsert_tagged_as(self, node_id: str, tag_id: str) -> None:
        edge_id = f"{node_id}__TAG__{tag_id}"
        self._submit(
            f"{self._g}.E('{_esc(edge_id)}').fold().coalesce("
            f"__.unfold(),"
            f"{self._g}.V('{_esc(node_id)}')"
            f".addE('TAGGED_AS')"
            f".to({self._g}.V('{_esc(tag_id)}'))"
            f".property('id','{_esc(edge_id)}')"
            f")"
        )

    # ------------------------------------------------------------------
    # Bulk load  (batched Gremlin upserts)
    # ------------------------------------------------------------------

    def begin_bulk(self) -> None:
        self._bulk_nodes = []
        self._bulk_edges = []
        log.debug("Cosmos bulk: buffers initialised")

    def write_bulk_node(self, record: BulkRecord) -> None:
        self._bulk_nodes.append(record)

    def write_bulk_edge(self, record: BulkRecord) -> None:
        self._bulk_edges.append(record)

    def commit_bulk(self) -> str:
        total_n = total_e = 0
        for i in range(0, len(self._bulk_nodes), self._bulk_batch):
            for rec in self._bulk_nodes[i : i + self._bulk_batch]:
                self.upsert_node(rec.entity_id, rec.label, rec.props)
                total_n += 1
        for i in range(0, len(self._bulk_edges), self._bulk_batch):
            for rec in self._bulk_edges[i : i + self._bulk_batch]:
                self.upsert_edge(
                    rec.entity_id, rec.label,
                    rec.from_id, rec.to_id, rec.props,
                )
                total_e += 1
        log.info("Cosmos bulk: upserted %d nodes, %d edges.", total_n, total_e)
        return ""

    def close(self) -> None:
        if self._client:
            self._client.close()
            log.info("Cosmos Gremlin client closed.")
