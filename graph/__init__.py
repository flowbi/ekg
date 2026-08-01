"""
graph  –  EKG graph target abstraction layer.

Exports the GraphTarget ABC and the factory function that instantiates
the correct implementation from a target name string.

Supported targets
─────────────────
  neptune   AWS Neptune  (Gremlin HTTP + S3 Bulk Loader)
  neo4j     Neo4j        (Bolt driver + LOAD CSV bulk via neo4j-admin)
  cosmos    Azure Cosmos DB for Apache Gremlin (Gremlin WebSocket + batched upserts)
  spanner   Google Spanner Graph (client library + LOAD DATA mutations)
  gml       GML file     (networkx; no live database — the file is the target)
  graphml   GraphML file (networkx; no live database — more consistently
                          supported across third-party viewers than gml)
"""

from graph.base import GraphTarget, BulkRecord, GraphTargetConfig, PreCheckError
from graph.neptune  import NeptuneGraphTarget
from graph.neo4j    import Neo4jGraphTarget
from graph.cosmos   import CosmosGraphTarget
from graph.spanner  import SpannerGraphTarget
from graph.gml      import GmlGraphTarget
from graph.graphml  import GraphMlGraphTarget

_REGISTRY: dict = {
    "neptune": NeptuneGraphTarget,
    "neo4j":   Neo4jGraphTarget,
    "cosmos":  CosmosGraphTarget,
    "spanner": SpannerGraphTarget,
    "gml":     GmlGraphTarget,
    "graphml": GraphMlGraphTarget,
}


def create_graph_target(target_name: str, config: GraphTargetConfig) -> GraphTarget:
    """
    Instantiate and return the GraphTarget implementation for *target_name*.

    Parameters
    ----------
    target_name : str
        One of 'neptune', 'neo4j', 'cosmos', 'spanner', 'gml', 'graphml'.
    config : GraphTargetConfig
        Target-specific configuration dataclass.

    Raises
    ------
    ValueError
        If *target_name* is not registered.
    """
    key = target_name.lower().strip()
    if key not in _REGISTRY:
        raise ValueError(
            f"Unknown graph target '{target_name}'. "
            f"Supported: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[key](config)


__all__ = [
    "GraphTarget",
    "BulkRecord",
    "GraphTargetConfig",
    "PreCheckError",
    "create_graph_target",
]
