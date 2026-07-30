"""
graph/cosmos.py  –  Azure Cosmos DB for Apache Gremlin target.

Incremental writes  →  Gremlin WebSocket via gremlin-python Client
Bulk load           →  Batched Gremlin upserts (Cosmos DB has no native
                        bulk-import API for the Gremlin surface).

Connection pattern
──────────────────
Cosmos DB for Gremlin requires credentials passed directly to the Client
constructor via username= and password=, combined with a PlainTextSASLAuthentication
transport factory.  The generic DriverRemoteConnection approach does not work
with Cosmos DB because it expects credentials in a different format.

Required options (GraphTargetConfig.options)
────────────────────────────────────────────
  endpoint        str   Gremlin WebSocket endpoint.
                        The portal shows: wss://my-account.gremlin.cosmos.azure.com:443/
                        The /gremlin path suffix is appended automatically.
  username        str   Graph path: /dbs/<database>/colls/<graph>
                        Find in Azure Portal → Data Explorer → your graph container.
  password        str   Primary key from Azure Portal → Keys → PRIMARY KEY
  bulk_batch      int   Upserts per batch during bulk load (default 100)
  message_timeout int   Seconds to wait for a Gremlin response (default 30)
  partition_key       str   Name of the container's partition key property
                            (Azure Portal → your graph → Settings → Partition
                            Key, e.g. "/partitionKey" → "partitionKey"). Every
                            vertex is written with this property set to
                            partition_key_value, since Cosmos DB Gremlin
                            rejects vertices where the partition key property
                            is missing/null. Leave empty to disable (only
                            valid for non-partitioned/legacy containers).
  partition_key_value str   Fixed value written to partition_key on every
                            vertex. Derived in ekg_etl.py from the tenant,
                            i.e. "{gsr_client}:{gsr_inst}", so all data for a
                            given client/instance lands in the same
                            partition. Required when partition_key is set.

Dependencies
────────────
  pip install gremlinpython
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from graph.base import BulkRecord, GraphTarget, GraphTargetConfig, PreCheckError

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


def _build_cosmos_client(endpoint: str, username: str, password: str, timeout: int):
    """
    Build a gremlinpython Client configured for Cosmos DB authentication.

    Cosmos DB requires SASL PLAIN authentication passed via a transport
    factory.  The standard Client(url, traversal_source, username=, password=)
    signature works when combined with the correct serializer; no
    DriverRemoteConnection is used.
    """
    from gremlin_python.driver import client as gremlin_client   # type: ignore
    from gremlin_python.driver import serializer                  # type: ignore
    from gremlin_python.driver.protocol import GremlinServerError  # type: ignore

    return gremlin_client.Client(
        endpoint,
        "g",
        username=username,
        password=password,
        message_serializer=serializer.GraphSONSerializersV2d0(),
    ), GremlinServerError


class CosmosGraphTarget(GraphTarget):
    """
    Azure Cosmos DB for Apache Gremlin implementation of GraphTarget.

    Upsert pattern uses coalesce(unfold, addV/addE) for idempotency.
    Cosmos DB Gremlin supports only single-cardinality properties;
    partition key property must be included in props for partitioned graphs.
    """

    def __init__(self, config: GraphTargetConfig) -> None:
        super().__init__(config)

        endpoint = config.get("endpoint", "").strip().rstrip("/")
        username = config.get("username", "")
        password = config.get("password", "")
        self._bulk_batch     = config.get("bulk_batch", 100)
        self._message_timeout = config.get("message_timeout", 30)
        self._partition_key       = config.get("partition_key", "")
        self._partition_key_value = config.get("partition_key_value", "")

        if self._partition_key and not self._partition_key_value:
            raise ValueError(
                "Cosmos DB partition_key is set but partition_key_value is empty. "
                "partition_key_value is derived from the tenant (gsr_client:gsr_inst) "
                "in ekg_etl.py — check TenantConfig/GSR_CLIENT_ID/GSR_INST_ID."
            )

        # ------------------------------------------------------------------
        # Validate endpoint
        # ------------------------------------------------------------------
        if not endpoint:
            raise ValueError(
                "Cosmos DB endpoint is not configured. "
                "Set EKG_TARGET_ENDPOINT to the Gremlin WebSocket URL, e.g.: "
                "wss://my-account.gremlin.cosmos.azure.com:443/"
            )
        if not endpoint.startswith("wss://"):
            raise ValueError(
                f"Cosmos DB Gremlin endpoint must start with 'wss://', got: '{endpoint}'. "
                "Correct format: wss://my-account.gremlin.cosmos.azure.com:443/"
            )

        # gremlinpython requires the /gremlin path suffix
        if not endpoint.endswith("/gremlin"):
            endpoint = f"{endpoint}/gremlin"
            log.debug("Appended /gremlin suffix: %s", endpoint)

        # ------------------------------------------------------------------
        # Validate credentials
        # ------------------------------------------------------------------
        if not username:
            raise ValueError(
                "Cosmos DB Gremlin username is not set. "
                "It must be the graph path: /dbs/<database>/colls/<graph>. "
                "Find this in Azure Portal → your Cosmos DB account → Data Explorer."
            )
        if not username.startswith("/dbs/"):
            raise ValueError(
                f"Cosmos DB Gremlin username must start with '/dbs/', got: '{username}'. "
                "Correct format: /dbs/<database>/colls/<graph>"
            )
        if not password:
            raise ValueError(
                "Cosmos DB Gremlin password (primary key) is not set. "
                "Find it in Azure Portal → your Cosmos DB account → Keys → PRIMARY KEY."
            )

        log.info("Cosmos DB Gremlin: connecting to %s (user=%s)", endpoint, username)

        self._client, self._GremlinServerError = _build_cosmos_client(
            endpoint, username, password, self._message_timeout
        )

        # Bulk buffers
        self._bulk_nodes: List[BulkRecord] = []
        self._bulk_edges: List[BulkRecord] = []

        log.info("Cosmos DB Gremlin target ready.")

    # ------------------------------------------------------------------
    # Precheck
    # ------------------------------------------------------------------

    def _do_precheck(self) -> None:
        """
        Submit g.V().limit(0) — zero-cost query confirming WebSocket
        connectivity and that the primary key is accepted.
        """
        log.debug("Cosmos precheck: g.V().limit(0)")
        try:
            callback = self._client.submitAsync("g.V().limit(0)")
            result   = callback.result()
        except Exception as exc:
            raise PreCheckError(
                f"Cosmos DB Gremlin precheck failed — could not submit query: {exc}. "
                "Check endpoint URL, username (/dbs/<db>/colls/<graph>), and primary key."
            ) from exc

        status = int(result.status_attributes.get("x-ms-status-code", 200))
        if status == 401:
            raise PreCheckError(
                "Cosmos DB Gremlin precheck: authentication failed (HTTP 401). "
                "Verify the primary key in your credentials.ini or secrets provider."
            )
        if status == 404:
            raise PreCheckError(
                "Cosmos DB Gremlin precheck: graph not found (HTTP 404). "
                f"Verify the username path matches an existing graph: '{self._client._username}'"
            )
        if status >= 400:
            raise PreCheckError(
                f"Cosmos DB Gremlin precheck query failed with status {status}. "
                f"Response attributes: {result.status_attributes}"
            )
        log.info("Cosmos DB Gremlin precheck OK (status %s).", status)

    # ------------------------------------------------------------------
    # Internal Gremlin execution
    # ------------------------------------------------------------------

    # Conflict statuses from Cosmos DB's optimistic concurrency control.
    # The coalesce(unfold(), addV(...)) upsert pattern is a read-then-write;
    # if the target vertex's document is touched between those two steps
    # (e.g. by the pipeline's own prior write still settling), Cosmos
    # rejects the write with 409/412. Retrying is safe because the whole
    # traversal re-evaluates current state from scratch.
    _CONFLICT_STATUSES = (409, 412)
    _MAX_CONFLICT_RETRIES = 4

    def _submit(self, query: str) -> None:
        attempt = 0
        while True:
            attempt += 1
            try:
                callback = self._client.submitAsync(query)
                result   = callback.result()
            except self._GremlinServerError as exc:
                if (
                    exc.status_code in self._CONFLICT_STATUSES
                    and attempt < self._MAX_CONFLICT_RETRIES
                ):
                    log.warning(
                        "Cosmos Gremlin conflict (status %s), retrying (attempt %d/%d): %s",
                        exc.status_code, attempt, self._MAX_CONFLICT_RETRIES, exc,
                    )
                    time.sleep(0.25 * attempt)
                    continue
                raise
            status = int(result.status_attributes.get("x-ms-status-code", 200))
            if status in self._CONFLICT_STATUSES and attempt < self._MAX_CONFLICT_RETRIES:
                log.warning(
                    "Cosmos Gremlin conflict (status %d), retrying (attempt %d/%d).",
                    status, attempt, self._MAX_CONFLICT_RETRIES,
                )
                time.sleep(0.25 * attempt)
                continue
            if status >= 400:
                raise RuntimeError(
                    f"Cosmos Gremlin error (status {status}): {result.status_attributes}"
                )
            return

    # ------------------------------------------------------------------
    # Incremental operations
    # ------------------------------------------------------------------

    def upsert_node(self, node_id: str, label: str, props: Dict[str, Any]) -> None:
        # Partition key is immutable once set, so it can only be written on the
        # addV() (create) branch — never on the shared tail, which also runs
        # for the unfold() (already-exists) branch and would try to rewrite it.
        pk_clause = (
            f".property('{self._partition_key}','{_esc(self._partition_key_value)}')"
            if self._partition_key else ""
        )
        self._submit(
            f"g.V('{_esc(node_id)}').fold().coalesce("
            f"__.unfold(),"
            f"__.addV('{_esc(label)}').property('id','{_esc(node_id)}'){pk_clause}"
            f"){_prop_chain(props)}"
        )

    def upsert_edge(
        self, edge_id: str, label: str, from_id: str, to_id: str, props: Dict[str, Any]
    ) -> None:
        self._submit(
            f"g.E('{_esc(edge_id)}').fold().coalesce("
            f"__.unfold(),"
            f"g.V('{_esc(from_id)}')"
            f".addE('{_esc(label)}')"
            f".to(g.V('{_esc(to_id)}'))"
            f".property('id','{_esc(edge_id)}')"
            f"){_prop_chain(props)}"
        )

    def delete_vertex(self, node_id: str) -> None:
        self._submit(f"g.V('{_esc(node_id)}').drop()")

    def delete_edge(self, edge_id: str) -> None:
        self._submit(f"g.E('{_esc(edge_id)}').drop()")

    def upsert_concept_tag(
        self, tag_id: str, tag_name: str, tag_category: str, display_name: Optional[str]
    ) -> None:
        dn = display_name or tag_name
        pk_clause = (
            f".property('{self._partition_key}','{_esc(self._partition_key_value)}')"
            if self._partition_key else ""
        )
        self._submit(
            f"g.V('{_esc(tag_id)}').fold().coalesce("
            f"__.unfold(),"
            f"__.addV('ConceptTag').property('id','{_esc(tag_id)}'){pk_clause}"
            f").property('name','{_esc(tag_name)}')"
            f".property('category','{_esc(tag_category)}')"
            f".property('display_name','{_esc(dn)}')"
        )

    def upsert_tagged_as(self, node_id: str, tag_id: str) -> None:
        edge_id = f"{node_id}__TAG__{tag_id}"
        self._submit(
            f"g.E('{_esc(edge_id)}').fold().coalesce("
            f"__.unfold(),"
            f"g.V('{_esc(node_id)}')"
            f".addE('TAGGED_AS')"
            f".to(g.V('{_esc(tag_id)}'))"
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
            log.debug("Cosmos bulk: flushed %d/%d nodes", total_n, len(self._bulk_nodes))

        for i in range(0, len(self._bulk_edges), self._bulk_batch):
            for rec in self._bulk_edges[i : i + self._bulk_batch]:
                self.upsert_edge(
                    rec.entity_id, rec.label,
                    rec.from_id, rec.to_id, rec.props,
                )
                total_e += 1
            log.debug("Cosmos bulk: flushed %d/%d edges", total_e, len(self._bulk_edges))

        log.info("Cosmos bulk: committed %d nodes, %d edges.", total_n, total_e)
        return ""

    def close(self) -> None:
        if self._client:
            self._client.close()
            log.info("Cosmos Gremlin client closed.")
