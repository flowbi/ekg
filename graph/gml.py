"""
graph/gml.py  –  GML (Graph Modelling Language) file graph target.

There is no live database here — the output file *is* the target. The full
graph is held in memory (as a networkx MultiDiGraph) for the duration of a
run and materialised to disk on close()/commit_bulk().

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
  endpoint   str   Path to the .gml output file, e.g. "./output/ekg.gml"
                    (EKG_TARGET_ENDPOINT). The parent directory is created
                    if it doesn't exist.

GML node/edge identity
───────────────────────
GML requires integer node ids at the file level; networkx assigns those
transparently on write and uses each node's GML `label` field to store
`str(node_key)` for round-tripping. We use our own string ~id (node_id /
edge_id) directly as the networkx node/edge key, so a node's GML `label`
field ends up holding exactly that id string — which is how the existing
file is re-keyed correctly on load (read_gml(..., label='label')).

Because GML's own `label` keyword is used this way, our *semantic* node/edge
label (e.g. "Customer", "REFERENCES") is stored under a separate attribute,
`entity_label`, to avoid colliding with it.

Dependencies
────────────
  pip install networkx
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from graph.base import BulkRecord, GraphTarget, GraphTargetConfig, PreCheckError

log = logging.getLogger("ekg_etl.graph.gml")

_KEY_RE = re.compile(r"[^A-Za-z0-9_]")


def _sanitize_key(key: str) -> str:
    """
    GML keys must match networkx's `^[A-Za-z][0-9A-Za-z_]*$` — alphanumeric
    or underscore, but the *first* character must be a letter (a leading
    digit or underscore, e.g. "_DeletedDateTS_", is rejected). Property
    names coming from attribute_mapping are user-authored and not
    guaranteed to satisfy this, so sanitize defensively.
    """
    sanitized = _KEY_RE.sub("_", key)
    if not sanitized or not sanitized[0].isalpha():
        sanitized = f"k_{sanitized}"
    return sanitized


def _sanitize_value(value: Any) -> Any:
    """
    Coerce every scalar to a string.

    Some GML viewers (Gephi in particular) infer a property's type from the
    first value they see for that name and then enforce it for every other
    node/edge in the file. But the same property name is shared across
    different entity mappings here, and different entities can back it with
    different source column types — e.g. one hub's own key column is a Long
    while another's is a padded CHAR string, or a shared property like
    "address_1" is a street-name string for one entity and a plain house
    number (Long) for another. Since properties are written one entity at a
    time with no view of what other entities will contribute under the same
    name, the only way to guarantee a single consistent type per name across
    the whole graph is to normalize every scalar value to a string up front
    (this also covers DB-native types psycopg2 returns for DATE/TIMESTAMP/
    NUMERIC columns — date/datetime/Decimal — which networkx's GML writer
    can't serialize natively either).
    """
    if isinstance(value, (dict, list, tuple)):
        return value
    return str(value)


def _sanitize_props(props: Dict[str, Any]) -> Dict[str, Any]:
    return {
        _sanitize_key(k): _sanitize_value(v)
        for k, v in props.items()
        if v is not None
    }


class GmlGraphTarget(GraphTarget):
    """
    File-based GraphTarget that materializes the graph as GML.

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
                "GML output path is not configured. "
                "Set EKG_TARGET_ENDPOINT to a file path, e.g. ./output/ekg.gml"
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
            "GML target ready: %s (%d existing node(s), %d existing edge(s))",
            self._path, self._graph.number_of_nodes(), self._graph.number_of_edges(),
        )

    # ------------------------------------------------------------------
    # Precheck
    # ------------------------------------------------------------------

    def _do_precheck(self) -> None:
        """Confirm the output directory is writable by touching a temp file."""
        out_dir = os.path.dirname(os.path.abspath(self._path)) or "."
        probe = os.path.join(out_dir, ".ekg_gml_write_test")
        try:
            with open(probe, "w") as f:
                f.write("")
            os.remove(probe)
        except OSError as exc:
            raise PreCheckError(
                f"GML output directory is not writable: {out_dir}: {exc}"
            ) from exc
        log.info("GML precheck OK: %s is writable.", out_dir)

    # ------------------------------------------------------------------
    # Existing-file loading / flushing
    # ------------------------------------------------------------------

    def _load_existing(self):
        if not os.path.exists(self._path):
            return self._nx.MultiDiGraph()
        try:
            loaded = self._nx.read_gml(self._path, label="label")
        except Exception as exc:
            log.warning(
                "Could not read existing GML file %s (%s); starting from an empty graph.",
                self._path, exc,
            )
            return self._nx.MultiDiGraph()

        # The file may have been written as a plain DiGraph (see _flush()) or,
        # if it had parallel edges, as a MultiDiGraph. Normalise back to a
        # MultiDiGraph either way, using each edge's 'ekg_id' property (not
        # networkx's own multigraph 'key', which a plain DiGraph doesn't have)
        # as the canonical edge identity.
        graph = self._nx.MultiDiGraph()
        graph.add_nodes_from(loaded.nodes(data=True))
        if loaded.is_multigraph():
            edges = loaded.edges(keys=True, data=True)
        else:
            edges = ((u, v, None, data) for u, v, data in loaded.edges(data=True))
        for u, v, key, data in edges:
            edge_id = data.get("ekg_id") or key or f"{u}->{v}"
            graph.add_edge(u, v, key=edge_id, **data)
        return graph

    def _flush(self) -> None:
        # networkx unconditionally emits a graph-level 'multigraph 1' flag and
        # a per-edge 'key' field for any MultiDiGraph, regardless of whether
        # parallel edges actually exist. That's a networkx-specific GML
        # extension, not part of the original spec, and plenty of third-party
        # readers (e.g. Graphia) fail to open a file that uses it. Only pay
        # that compatibility cost when the graph genuinely needs it.
        pairs = [(u, v) for u, v in self._graph.edges()]
        has_parallel_edges = len(pairs) != len(set(pairs))
        if has_parallel_edges:
            log.warning(
                "GML: this graph has parallel edges (multiple edges between "
                "the same node pair), so the file is written with networkx's "
                "multigraph GML extension ('multigraph 1' + per-edge 'key'). "
                "Some GML viewers don't support that extension and may fail "
                "to open the file."
            )
            self._nx.write_gml(self._graph, self._path)
        else:
            plain = self._nx.DiGraph()
            plain.add_nodes_from(self._graph.nodes(data=True))
            plain.add_edges_from(self._graph.edges(data=True))
            self._nx.write_gml(plain, self._path)
        log.info(
            "GML: wrote %s (%d node(s), %d edge(s)).",
            self._path, self._graph.number_of_nodes(), self._graph.number_of_edges(),
        )

    # ------------------------------------------------------------------
    # Incremental operations
    # ------------------------------------------------------------------

    def upsert_node(self, node_id: str, label: str, props: Dict[str, Any]) -> None:
        self._graph.add_node(node_id, entity_label=label, **_sanitize_props(props))

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
            from_id, to_id, key=edge_id, entity_label=label, ekg_id=edge_id,
            **_sanitize_props(props),
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
        log.debug("GML bulk: starting from an empty graph")

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
            "GML bulk: committed %d node(s), %d edge(s).",
            len(self._bulk_nodes), len(self._bulk_edges),
        )
        return ""

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        self._flush()
