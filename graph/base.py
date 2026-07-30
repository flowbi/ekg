"""
graph/base.py  –  GraphTarget abstract base class and shared types.

Every graph target implementation must subclass GraphTarget and implement
all abstract methods.  The EKGETLPipeline calls only these methods; it has
no knowledge of which target is active.

Precheck lifecycle
──────────────────
  precheck()        – verify connectivity and authentication before the pipeline
                      starts.  Calls _do_precheck() on the implementation.
                      Raises PreCheckError on failure, aborting the pipeline.

Bulk-load lifecycle
───────────────────
  begin_bulk()                    – prepare buffers / connections / staging area
  write_bulk_node(BulkRecord)     – accumulate one node record
  write_bulk_edge(BulkRecord)     – accumulate one edge record
  commit_bulk() -> str            – finalise and trigger the load;
                                    returns an opaque job-ID string (may be "")
  close()                         – release all resources

Incremental lifecycle (per row)
───────────────────────────────
  upsert_node(id, label, props)
  upsert_edge(id, label, from_id, to_id, props)
  delete_vertex(id)
  delete_edge(id)
  upsert_concept_tag(tag_id, name, category, display_name)
  upsert_tagged_as(node_id, tag_id)
  close()
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

log = logging.getLogger("ekg_etl.graph")


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class PreCheckError(RuntimeError):
    """
    Raised by GraphTarget.precheck() when connectivity or authentication
    to the target graph database cannot be confirmed.

    The pipeline catches this at startup and aborts immediately with a
    clear log message rather than failing silently mid-run.
    """


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class GraphTargetConfig:
    """
    Generic configuration container for any graph target.

    Target implementations read only the keys they need from *options*.
    Common keys used by multiple targets are documented below; target-specific
    keys are documented in each implementation file.

    Common options keys
    ───────────────────
    endpoint        str   Primary connection endpoint (URL, host, or DSN)
    port            int   Port number (where applicable)
    database        str   Database / keyspace / graph name
    username        str   Auth username (resolved at runtime via secrets)
    password        str   Auth password (resolved at runtime via secrets)
    secret_ref      str   Secrets-provider reference for credential resolution
    region          str   Cloud region (AWS / Azure / GCP)
    bulk_staging    str   Staging location for bulk load (S3 URI, GCS URI, etc.)
    iam_role_arn    str   IAM role for bulk operations (Neptune / Spanner)
    tls             bool  Require TLS (default True)
    """
    target:  str                      # 'neptune' | 'neo4j' | 'cosmos' | 'spanner'
    options: Dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        return self.options.get(key, default)


# ---------------------------------------------------------------------------
# Bulk record
# ---------------------------------------------------------------------------

@dataclass
class BulkRecord:
    """
    Represents a single node or edge to be written during a bulk load.

    Attributes
    ----------
    entity_type : str
        'node' or 'edge'.
    entity_id : str
        Graph ~id value.
    label : str
        Node or edge label.
    props : dict
        Property key → value mapping (includes __privacy_class companions).
    from_id : str | None
        Source node ID (edges only).
    to_id : str | None
        Target node ID (edges only).
    """
    entity_type: str                    # 'node' | 'edge'
    entity_id:   str
    label:       str
    props:       Dict[str, Any]
    from_id:     Optional[str] = None   # edges only
    to_id:       Optional[str] = None   # edges only


# ---------------------------------------------------------------------------
# Abstract base class
# ---------------------------------------------------------------------------

class GraphTarget(ABC):
    """
    Abstract base class for all EKG graph target implementations.

    Implementations must be thread-safe within a single pipeline run
    (the pipeline is single-threaded, so this is not a strict requirement,
    but implementations should not share mutable state across instances).
    """

    def __init__(self, config: GraphTargetConfig) -> None:
        self._config = config

    # ------------------------------------------------------------------
    # Precheck  (concrete wrapper + abstract implementation)
    # ------------------------------------------------------------------

    def precheck(self) -> None:
        """
        Verify connectivity and authentication to the target graph database.

        Called once at pipeline startup, before any data is read from
        source databases.  Raises PreCheckError on failure, which the
        pipeline catches and re-raises to abort immediately.

        Subclasses implement _do_precheck() with target-specific logic.
        """
        target = self._config.target
        log.info("Precheck: verifying connectivity to %s graph target …", target)
        try:
            self._do_precheck()
        except PreCheckError:
            raise   # already well-formed — let it propagate
        except Exception as exc:
            raise PreCheckError(
                f"Precheck failed for target '{target}': {exc}"
            ) from exc
        log.info("Precheck: %s graph target OK.", target)

    @abstractmethod
    def _do_precheck(self) -> None:
        """
        Target-specific connectivity and authentication check.

        Should make the lightest possible authenticated request that
        confirms both network reachability and credential validity.

        Raise any exception on failure; the precheck() wrapper converts
        it to a PreCheckError with context.
        """

    # ------------------------------------------------------------------
    # Incremental write operations
    # ------------------------------------------------------------------

    @abstractmethod
    def upsert_node(
        self,
        node_id: str,
        label:   str,
        props:   Dict[str, Any],
    ) -> None:
        """Insert or update a node. All properties in *props* are set."""

    @abstractmethod
    def upsert_edge(
        self,
        edge_id:  str,
        label:    str,
        from_id:  str,
        to_id:    str,
        props:    Dict[str, Any],
    ) -> None:
        """Insert or update an edge connecting *from_id* → *to_id*."""

    @abstractmethod
    def delete_vertex(self, node_id: str) -> None:
        """Hard-delete a node and all its incident edges."""

    @abstractmethod
    def delete_edge(self, edge_id: str) -> None:
        """Hard-delete a single edge."""

    @abstractmethod
    def upsert_concept_tag(
        self,
        tag_id:       str,
        tag_name:     str,
        tag_category: str,
        display_name: Optional[str],
    ) -> None:
        """Insert or update a ConceptTag node."""

    @abstractmethod
    def upsert_tagged_as(self, node_id: str, tag_id: str) -> None:
        """Insert or update a TAGGED_AS edge from a data node to a ConceptTag."""

    # ------------------------------------------------------------------
    # Bulk load lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    def begin_bulk(self) -> None:
        """Prepare for bulk load. Called once before any write_bulk_* calls."""

    @abstractmethod
    def write_bulk_node(self, record: BulkRecord) -> None:
        """Accumulate one node record during bulk load."""

    @abstractmethod
    def write_bulk_edge(self, record: BulkRecord) -> None:
        """Accumulate one edge record during bulk load."""

    @abstractmethod
    def commit_bulk(self) -> str:
        """
        Finalise the bulk load and trigger ingestion.
        Returns an opaque job-ID string (may be "" if target has no job ID).
        """

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    def close(self) -> None:
        """Release all connections and resources."""

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @property
    def target_name(self) -> str:
        return self._config.target
