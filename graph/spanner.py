"""
graph/spanner.py  –  Google Spanner Graph target.

Spanner Graph (preview) represents nodes and edges as regular Spanner tables
with NODE and EDGE table annotations in the schema DDL.  The EKG pipeline
treats these tables as upsert targets via the Spanner client library.

Incremental writes  →  Spanner client library INSERT OR UPDATE (upsert) mutations
Bulk load           →  Batched mutations grouped into transactions
                        (Spanner has no dedicated graph bulk-import API;
                        for very large initial loads consider Spanner
                        Dataflow templates or LOAD DATA FROM Google Cloud Storage)

Required options (GraphTargetConfig.options)
────────────────────────────────────────────
  project         str   GCP project ID
  instance        str   Spanner instance ID
  database        str   Spanner database ID
  node_table      str   Spanner table used for all nodes (default "EKGNode")
  edge_table      str   Spanner table used for all edges (default "EKGEdge")
  bulk_batch      int   Mutations per transaction during bulk load (default 1000)

Node table expected schema (minimum)
──────────────────────────────────────
  id          STRING(MAX)  NOT NULL
  label       STRING(256)  NOT NULL
  properties  JSON
  PRIMARY KEY (id)

Edge table expected schema (minimum)
──────────────────────────────────────
  id          STRING(MAX)  NOT NULL
  from_id     STRING(MAX)  NOT NULL
  to_id       STRING(MAX)  NOT NULL
  label       STRING(256)  NOT NULL
  properties  JSON
  PRIMARY KEY (id)

Dependencies
────────────
  pip install google-cloud-spanner
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from graph.base import BulkRecord, GraphTarget, GraphTargetConfig

log = logging.getLogger("ekg_etl.graph.spanner")


class SpannerGraphTarget(GraphTarget):
    """
    Google Spanner Graph implementation of GraphTarget.

    All writes use INSERT_OR_UPDATE mutations for idempotency.
    Properties are serialised to JSON and stored in a single JSON column;
    this simplifies the schema while allowing arbitrary property sets.
    """

    def __init__(self, config: GraphTargetConfig) -> None:
        super().__init__(config)
        try:
            from google.cloud import spanner  # type: ignore
        except ImportError as exc:
            raise ImportError("pip install google-cloud-spanner") from exc

        project    = config.get("project", "")
        instance   = config.get("instance", "")
        database   = config.get("database", "")
        self._node_table = config.get("node_table", "EKGNode")
        self._edge_table = config.get("edge_table", "EKGEdge")
        self._bulk_batch = config.get("bulk_batch", 1000)

        spanner_client   = spanner.Client(project=project)
        instance_obj     = spanner_client.instance(instance)
        self._db         = instance_obj.database(database)
        self._spanner    = spanner

        # Bulk load buffers
        self._bulk_node_mutations: list = []
        self._bulk_edge_mutations: list = []

        log.info(
            "Spanner target connected: project=%s instance=%s database=%s",
            project, instance, database,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _node_mutation(self, node_id: str, label: str, props: Dict[str, Any]):
        return self._spanner.param_types, {
            "id": node_id,
            "label": label,
            "properties": json.dumps(props, default=str),
        }

    def _upsert_node_row(self, node_id: str, label: str, props: Dict[str, Any]) -> None:
        def _txn(transaction):
            transaction.insert_or_update(
                self._node_table,
                columns=["id", "label", "properties"],
                values=[(node_id, label, json.dumps(props, default=str))],
            )
        self._db.run_in_transaction(_txn)

    def _upsert_edge_row(
        self, edge_id: str, label: str, from_id: str, to_id: str, props: Dict[str, Any]
    ) -> None:
        def _txn(transaction):
            transaction.insert_or_update(
                self._edge_table,
                columns=["id", "from_id", "to_id", "label", "properties"],
                values=[(edge_id, from_id, to_id, label, json.dumps(props, default=str))],
            )
        self._db.run_in_transaction(_txn)

    def _delete_node_row(self, node_id: str) -> None:
        def _txn(transaction):
            transaction.delete(self._node_table, self._spanner.KeySet(keys=[[node_id]]))
            # Also delete all incident edges
            transaction.execute_update(
                f"DELETE FROM {self._edge_table} "
                f"WHERE from_id = @id OR to_id = @id",
                params={"id": node_id},
                param_types={"id": self._spanner.param_types.STRING},
            )
        self._db.run_in_transaction(_txn)

    def _delete_edge_row(self, edge_id: str) -> None:
        def _txn(transaction):
            transaction.delete(self._edge_table, self._spanner.KeySet(keys=[[edge_id]]))
        self._db.run_in_transaction(_txn)

    # ------------------------------------------------------------------
    # Incremental operations
    # ------------------------------------------------------------------

    def upsert_node(self, node_id: str, label: str, props: Dict[str, Any]) -> None:
        self._upsert_node_row(node_id, label, props)

    def upsert_edge(
        self, edge_id: str, label: str, from_id: str, to_id: str, props: Dict[str, Any]
    ) -> None:
        self._upsert_edge_row(edge_id, label, from_id, to_id, props)

    def delete_vertex(self, node_id: str) -> None:
        self._delete_node_row(node_id)

    def delete_edge(self, edge_id: str) -> None:
        self._delete_edge_row(edge_id)

    def upsert_concept_tag(
        self, tag_id: str, tag_name: str, tag_category: str, display_name: Optional[str]
    ) -> None:
        props = {
            "name": tag_name,
            "category": tag_category,
            "display_name": display_name or tag_name,
        }
        self._upsert_node_row(tag_id, "ConceptTag", props)

    def upsert_tagged_as(self, node_id: str, tag_id: str) -> None:
        edge_id = f"{node_id}__TAG__{tag_id}"
        self._upsert_edge_row(edge_id, "TAGGED_AS", node_id, tag_id, {})

    # ------------------------------------------------------------------
    # Bulk load
    # ------------------------------------------------------------------

    def begin_bulk(self) -> None:
        self._bulk_node_mutations = []
        self._bulk_edge_mutations = []
        log.debug("Spanner bulk: buffers initialised")

    def write_bulk_node(self, record: BulkRecord) -> None:
        self._bulk_node_mutations.append(
            (record.entity_id, record.label, json.dumps(record.props, default=str))
        )

    def write_bulk_edge(self, record: BulkRecord) -> None:
        self._bulk_edge_mutations.append(
            (record.entity_id, record.from_id, record.to_id,
             record.label, json.dumps(record.props, default=str))
        )

    def commit_bulk(self) -> str:
        batch = self._bulk_batch

        # Flush nodes
        for i in range(0, len(self._bulk_node_mutations), batch):
            chunk = self._bulk_node_mutations[i : i + batch]
            def _node_txn(transaction, rows=chunk):
                transaction.insert_or_update(
                    self._node_table,
                    columns=["id", "label", "properties"],
                    values=rows,
                )
            self._db.run_in_transaction(_node_txn)
            log.debug("Spanner bulk: flushed %d node mutations", len(chunk))

        # Flush edges
        for i in range(0, len(self._bulk_edge_mutations), batch):
            chunk = self._bulk_edge_mutations[i : i + batch]
            def _edge_txn(transaction, rows=chunk):
                transaction.insert_or_update(
                    self._edge_table,
                    columns=["id", "from_id", "to_id", "label", "properties"],
                    values=rows,
                )
            self._db.run_in_transaction(_edge_txn)
            log.debug("Spanner bulk: flushed %d edge mutations", len(chunk))

        log.info(
            "Spanner bulk: committed %d nodes, %d edges.",
            len(self._bulk_node_mutations), len(self._bulk_edge_mutations),
        )
        return ""

    def close(self) -> None:
        # Spanner client library manages connection pooling internally
        log.info("Spanner target closed.")
