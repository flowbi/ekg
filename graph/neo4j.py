"""
graph/neo4j.py  –  Neo4j graph target.

Incremental writes  →  Neo4j Bolt driver (MERGE … SET)
Bulk load           →  Batched MERGE over Bolt in a single transaction per batch
                        (neo4j-admin import is a server-side tool and unavailable
                        in managed / Aura environments; batched MERGE is the
                        portable alternative.  Swap commit_bulk for neo4j-admin
                        if you have direct server access.)

Required options (GraphTargetConfig.options)
────────────────────────────────────────────
  endpoint      str   Bolt URI, e.g. "bolt://localhost:7687" or "neo4j+s://..."
  username      str   Neo4j username (resolved via secrets)
  password      str   Neo4j password (resolved via secrets)
  database      str   Target database name (default "neo4j")
  bulk_batch    int   Rows per MERGE batch during bulk load (default 500)

Dependencies
────────────
  pip install neo4j
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from graph.base import BulkRecord, GraphTarget, GraphTargetConfig

log = logging.getLogger("ekg_etl.graph.neo4j")


class Neo4jGraphTarget(GraphTarget):
    """
    Neo4j implementation of GraphTarget using the official Bolt driver.

    All upserts use MERGE on the element ID so that repeated runs are
    idempotent.  For nodes the ID is stored as a dedicated property
    `_ekg_id` in addition to being used in the MERGE predicate, because
    Neo4j does not expose a user-settable element id.
    """

    def __init__(self, config: GraphTargetConfig) -> None:
        super().__init__(config)
        try:
            from neo4j import GraphDatabase  # type: ignore
        except ImportError as exc:
            raise ImportError("pip install neo4j") from exc

        endpoint = config.get("endpoint", "bolt://localhost:7687")
        username = config.get("username", "neo4j")
        password = config.get("password", "")
        self._database   = config.get("database", "neo4j")
        self._bulk_batch = config.get("bulk_batch", 500)

        self._driver = GraphDatabase.driver(endpoint, auth=(username, password))

        # Bulk load buffers
        self._bulk_nodes: List[Dict] = []
        self._bulk_edges: List[Dict] = []

        log.info("Neo4j target connected to %s (db=%s)", endpoint, self._database)

    # ------------------------------------------------------------------
    # Incremental operations
    # ------------------------------------------------------------------

    def _run(self, cypher: str, **params) -> None:
        with self._driver.session(database=self._database) as session:
            session.run(cypher, **params)

    def upsert_node(self, node_id: str, label: str, props: Dict[str, Any]) -> None:
        all_props = {"_ekg_id": node_id, **props}
        self._run(
            f"MERGE (n:`{label}` {{_ekg_id: $id}}) SET n += $props",
            id=node_id, props=all_props,
        )

    def upsert_edge(
        self, edge_id: str, label: str, from_id: str, to_id: str, props: Dict[str, Any]
    ) -> None:
        all_props = {"_ekg_id": edge_id, **props}
        self._run(
            f"""
            MATCH (a {{_ekg_id: $from_id}})
            MATCH (b {{_ekg_id: $to_id}})
            MERGE (a)-[r:`{label}` {{_ekg_id: $id}}]->(b)
            SET r += $props
            """,
            from_id=from_id, to_id=to_id, id=edge_id, props=all_props,
        )

    def delete_vertex(self, node_id: str) -> None:
        self._run(
            "MATCH (n {_ekg_id: $id}) DETACH DELETE n",
            id=node_id,
        )

    def delete_edge(self, edge_id: str) -> None:
        self._run(
            "MATCH ()-[r {_ekg_id: $id}]->() DELETE r",
            id=edge_id,
        )

    def upsert_concept_tag(
        self, tag_id: str, tag_name: str, tag_category: str, display_name: Optional[str]
    ) -> None:
        self._run(
            """
            MERGE (t:ConceptTag {_ekg_id: $id})
            SET t.name         = $name,
                t.category     = $category,
                t.display_name = $display_name
            """,
            id=tag_id, name=tag_name, category=tag_category,
            display_name=display_name or tag_name,
        )

    def upsert_tagged_as(self, node_id: str, tag_id: str) -> None:
        edge_id = f"{node_id}__TAG__{tag_id}"
        self._run(
            """
            MATCH (n {_ekg_id: $node_id})
            MATCH (t:ConceptTag {_ekg_id: $tag_id})
            MERGE (n)-[r:TAGGED_AS {_ekg_id: $edge_id}]->(t)
            """,
            node_id=node_id, tag_id=tag_id, edge_id=edge_id,
        )

    # ------------------------------------------------------------------
    # Bulk load
    # ------------------------------------------------------------------

    def begin_bulk(self) -> None:
        self._bulk_nodes = []
        self._bulk_edges = []
        log.debug("Neo4j bulk: buffers initialised")

    def write_bulk_node(self, record: BulkRecord) -> None:
        self._bulk_nodes.append({
            "id":    record.entity_id,
            "label": record.label,
            "props": {"_ekg_id": record.entity_id, **record.props},
        })

    def write_bulk_edge(self, record: BulkRecord) -> None:
        self._bulk_edges.append({
            "id":      record.entity_id,
            "label":   record.label,
            "from_id": record.from_id,
            "to_id":   record.to_id,
            "props":   {"_ekg_id": record.entity_id, **record.props},
        })

    def commit_bulk(self) -> str:
        """
        Flush all buffered nodes and edges to Neo4j using batched MERGE
        transactions.  Returns "" (no external job ID).
        """
        self._flush_nodes()
        self._flush_edges()
        log.info(
            "Neo4j bulk: committed %d nodes, %d edges.",
            len(self._bulk_nodes), len(self._bulk_edges),
        )
        return ""

    def _flush_nodes(self) -> None:
        batch = self._bulk_batch
        for i in range(0, len(self._bulk_nodes), batch):
            chunk = self._bulk_nodes[i : i + batch]
            with self._driver.session(database=self._database) as session:
                session.run(
                    """
                    UNWIND $rows AS row
                    MERGE (n {_ekg_id: row.id})
                    SET n += row.props
                    """,
                    rows=chunk,
                )
            log.debug("Neo4j bulk: flushed %d/%d nodes", min(i + batch, len(self._bulk_nodes)), len(self._bulk_nodes))

    def _flush_edges(self) -> None:
        batch = self._bulk_batch
        for i in range(0, len(self._bulk_edges), batch):
            chunk = self._bulk_edges[i : i + batch]
            with self._driver.session(database=self._database) as session:
                session.run(
                    """
                    UNWIND $rows AS row
                    MATCH (a {_ekg_id: row.from_id})
                    MATCH (b {_ekg_id: row.to_id})
                    MERGE (a)-[r {_ekg_id: row.id}]->(b)
                    SET r += row.props
                    """,
                    rows=chunk,
                )
            log.debug("Neo4j bulk: flushed %d/%d edges", min(i + batch, len(self._bulk_edges)), len(self._bulk_edges))

    def close(self) -> None:
        if self._driver:
            self._driver.close()
            log.info("Neo4j driver closed.")
