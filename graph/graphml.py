"""
graph/graphml.py  –  GraphML file graph target.

There is no live database here — the output file *is* the target, same
model as graph/gml.py. This target exists because GML compatibility across
third-party viewers is inconsistent (networkx's GML writer's own extensions
for parallel edges aren't universally supported, and some importers are
stricter than others about property typing) — GraphML is an XML format with
a formal XSD schema and is more consistently supported. Prefer this target
over gml.py unless you specifically need GML.

Incremental writes  →  In-memory graph, mutated per upsert/delete. The
                        existing file (if present) is loaded at startup so
                        successive incremental runs build on prior state.
                        Changes are only written to disk at close() — a
                        full rewrite per row would be far too slow for
                        anything but a toy graph, so a crash mid-run loses
                        this run's changes but never corrupts the file from
                        the previous run.
Bulk load           →  Buffered like the other targets (begin_bulk /
                        write_bulk_node / write_bulk_edge). begin_bulk()
                        discards any existing in-memory/on-disk graph, since
                        a full reload replaces the file rather than merging
                        with it. commit_bulk() writes the file once.

Required options (GraphTargetConfig.options)
────────────────────────────────────────────
  endpoint   str   Path to the .graphml output file, e.g. "./output/ekg.graphml"
                    (EKG_TARGET_ENDPOINT). The parent directory is created
                    if it doesn't exist.

GraphML node/edge identity
────────────────────────────
Unlike GML, GraphML node/edge ids are arbitrary strings, so our own string
~id (node_id / edge_id) is used directly — no synthetic-integer-id
workaround needed. Parallel edges are handled natively by networkx's
GraphML reader/writer (each <edge> carries its own `id` attribute), so
unlike gml.py there's no dual write-path for the multigraph case.

Because GraphML doesn't reserve any property name the way GML reserves
`label`, our semantic node/edge label (e.g. "Customer", "REFERENCES") is
still stored under `entity_label` rather than `label`, purely to keep
property names consistent with the GML target for anyone switching between
the two.

Dependencies
────────────
  pip install networkx
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from graph.base import BulkRecord, GraphTarget, GraphTargetConfig, PreCheckError
from graph._props import sanitize_props

log = logging.getLogger("ekg_etl.graph.graphml")


class GraphMlGraphTarget(GraphTarget):
    """
    File-based GraphTarget that materializes the graph as GraphML.

    All upserts merge new properties onto existing ones (matching the
    "SET n += props" semantics of the Neo4j/Cosmos targets), rather than
    replacing a node/edge's property set wholesale.
    """

    def __init__(self, config: GraphTargetConfig) -> None:
        super().__init__(config)
        try:
            import networkx as nx  # type: ignore
        except ImportError as exc:
            raise ImportError("pip install networkx") from exc
        self._nx = nx

        path = config.get("endpoint", "").strip()
        if not path:
            raise ValueError(
                "GraphML output path is not configured. "
                "Set EKG_TARGET_ENDPOINT to a file path, e.g. ./output/ekg.graphml"
            )
        self._path = path

        out_dir = os.path.dirname(os.path.abspath(path))
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)

        self._graph = self._load_existing()
        # edge_id -> (from_id, to_id), so delete_edge() doesn't need to scan
        # every edge in the graph to find the one to remove.
        self._edge_endpoints: Dict[str, Tuple[str, str]] = {
            key: (u, v) for u, v, key in self._graph.edges(keys=True)
        }

        # Bulk load buffers
        self._bulk_nodes: List[BulkRecord] = []
        self._bulk_edges: List[BulkRecord] = []

        log.info(
            "GraphML target ready: %s (%d existing node(s), %d existing edge(s))",
            self._path, self._graph.number_of_nodes(), self._graph.number_of_edges(),
        )

    # ------------------------------------------------------------------
    # Precheck
    # ------------------------------------------------------------------

    def _do_precheck(self) -> None:
        """Confirm the output directory is writable by touching a temp file."""
        out_dir = os.path.dirname(os.path.abspath(self._path)) or "."
        probe = os.path.join(out_dir, ".ekg_graphml_write_test")
        try:
            with open(probe, "w") as f:
                f.write("")
            os.remove(probe)
        except OSError as exc:
            raise PreCheckError(
                f"GraphML output directory is not writable: {out_dir}: {exc}"
            ) from exc
        log.info("GraphML precheck OK: %s is writable.", out_dir)

    # ------------------------------------------------------------------
    # Existing-file loading / flushing
    # ------------------------------------------------------------------

    def _load_existing(self):
        if not os.path.exists(self._path):
            return self._nx.MultiDiGraph()
        try:
            loaded = self._nx.read_graphml(self._path)
        except Exception as exc:
            log.warning(
                "Could not read existing GraphML file %s (%s); starting from an empty graph.",
                self._path, exc,
            )
            return self._nx.MultiDiGraph()

        # read_graphml returns a plain DiGraph when the file has no parallel
        # edges, or a MultiDiGraph (with our own edge ids as keys, since
        # GraphML's <edge id="..."> is preserved as the multigraph key) when
        # it does. Normalise to MultiDiGraph either way; when non-multi, the
        # edge id still survives as a plain 'id' data field.
        graph = self._nx.MultiDiGraph()
        graph.add_nodes_from(loaded.nodes(data=True))
        if loaded.is_multigraph():
            edges = loaded.edges(keys=True, data=True)
        else:
            edges = ((u, v, None, data) for u, v, data in loaded.edges(data=True))
        for u, v, key, data in edges:
            edge_id = data.pop("id", None) or key or f"{u}->{v}"
            graph.add_edge(u, v, key=edge_id, **data)
        return graph

    def _flush(self) -> None:
        self._nx.write_graphml(self._graph, self._path)
        log.info(
            "GraphML: wrote %s (%d node(s), %d edge(s)).",
            self._path, self._graph.number_of_nodes(), self._graph.number_of_edges(),
        )

    # ------------------------------------------------------------------
    # Incremental operations
    # ------------------------------------------------------------------

    def upsert_node(self, node_id: str, label: str, props: Dict[str, Any]) -> None:
        self._graph.add_node(node_id, entity_label=label, **sanitize_props(props))

    def upsert_edge(
        self, edge_id: str, label: str, from_id: str, to_id: str, props: Dict[str, Any]
    ) -> None:
        if not self._graph.has_node(from_id) or not self._graph.has_node(to_id):
            log.warning(
                "Edge %s (%s) NOT created — from_id=%s (exists=%s) to_id=%s (exists=%s)",
                edge_id, label, from_id, self._graph.has_node(from_id),
                to_id, self._graph.has_node(to_id),
            )
            return
        self._graph.add_edge(
            from_id, to_id, key=edge_id, entity_label=label, **sanitize_props(props),
        )
        self._edge_endpoints[edge_id] = (from_id, to_id)

    def delete_vertex(self, node_id: str) -> None:
        if self._graph.has_node(node_id):
            for u, v, key in list(self._graph.edges(node_id, keys=True)):
                self._edge_endpoints.pop(key, None)
            self._graph.remove_node(node_id)

    def delete_edge(self, edge_id: str) -> None:
        endpoints = self._edge_endpoints.pop(edge_id, None)
        if endpoints is None:
            return
        u, v = endpoints
        if self._graph.has_edge(u, v, key=edge_id):
            self._graph.remove_edge(u, v, key=edge_id)

    def upsert_concept_tag(
        self, tag_id: str, tag_name: str, tag_category: str, display_name: Optional[str]
    ) -> None:
        self._graph.add_node(
            tag_id,
            entity_label="ConceptTag",
            name=tag_name,
            category=tag_category,
            display_name=display_name or tag_name,
        )

    def upsert_tagged_as(self, node_id: str, tag_id: str) -> None:
        edge_id = f"{node_id}__TAG__{tag_id}"
        self.upsert_edge(edge_id, "TAGGED_AS", node_id, tag_id, {})

    # ------------------------------------------------------------------
    # Bulk load
    # ------------------------------------------------------------------

    def begin_bulk(self) -> None:
        # A full reload replaces the file — start from an empty graph
        # rather than merging with whatever was loaded from disk.
        self._graph = self._nx.MultiDiGraph()
        self._edge_endpoints = {}
        self._bulk_nodes = []
        self._bulk_edges = []
        log.debug("GraphML bulk: starting from an empty graph")

    def write_bulk_node(self, record: BulkRecord) -> None:
        self._bulk_nodes.append(record)

    def write_bulk_edge(self, record: BulkRecord) -> None:
        self._bulk_edges.append(record)

    def commit_bulk(self) -> str:
        for rec in self._bulk_nodes:
            self.upsert_node(rec.entity_id, rec.label, rec.props)
        for rec in self._bulk_edges:
            self.upsert_edge(rec.entity_id, rec.label, rec.from_id, rec.to_id, rec.props)
        self._flush()
        log.info(
            "GraphML bulk: committed %d node(s), %d edge(s).",
            len(self._bulk_nodes), len(self._bulk_edges),
        )
        return ""

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        self._flush()
