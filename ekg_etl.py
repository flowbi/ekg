"""
ekg_etl.py  –  Enterprise Knowledge Graph ETL pipeline  (v5)
=============================================================
Graph-neutral incremental ETL: relational source databases → any supported
EKG graph target (AWS Neptune, Neo4j, Azure Cosmos DB, Google Spanner Graph).

Module layout
─────────────
  ekg_etl.py          This file – pipeline orchestrator only.
  queries.ini         All SQL statements (external, editable without code changes).
  graph/              Graph target abstraction layer.
    base.py             GraphTarget ABC + BulkRecord + GraphTargetConfig
    neptune.py          AWS Neptune
    neo4j.py            Neo4j
    cosmos.py           Azure Cosmos DB for Apache Gremlin
    spanner.py          Google Spanner Graph
  secrets/            Credential resolution abstraction layer.
    base.py             SecretsProvider ABC + ChainedSecretsProvider + factory
    aws.py              AWS Secrets Manager
    azure.py            Azure Key Vault
    google.py           Google Secret Manager
    vault.py            HashiCorp Vault (KV v2)
    env.py              Environment variables (universal fallback)
  logging_/           Logging handler abstraction layer.
    builder.py          LoggingBuilder + configure_logging()
    cloudwatch.py       AWS CloudWatch Logs
    azure_monitor.py    Azure Monitor / Application Insights
    google_logging.py   Google Cloud Logging

Secrets chain
─────────────
  Set EKG_SECRET_PROVIDERS to a comma-separated list of providers in
  priority order (e.g. "aws,env" or "vault,azure,env").
  Supported: aws, azure, google, vault, env
  'env' is always appended as the final fallback.

Logging targets
───────────────
  Set EKG_LOG_TARGETS to a comma-separated list (e.g. "console,cloudwatch").
  Supported: console, cloudwatch, azure, google
  Console is always added when no cloud handler installs successfully.

Graph target
────────────
  Set EKG_GRAPH_TARGET to one of: neptune, neo4j, cosmos, spanner, gml, graphml.
  Target-specific options are read from env vars prefixed EKG_TARGET_*.

Run modes
─────────
  full          Initial / full reload via each target's bulk-load mechanism.
  incremental   Change detection + graph upserts / hard deletes.

CLI
───
  python ekg_etl.py --mode incremental
  python ekg_etl.py --mode full --debug
"""

from __future__ import annotations

import csv
import hashlib
import importlib
import io
import json
import logging
import os
import glob
import re
import sys
import time
import uuid
import configparser
import jaydebeapi
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple


import psycopg2
import psycopg2.extras

# Internal modules
from graph       import GraphTarget, GraphTargetConfig, BulkRecord, create_graph_target
from secrets     import build_secrets_chain, ChainedSecretsProvider
from logging_    import configure_logging

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

QUERIES_FILE = Path(__file__).parent / "queries.ini"

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

EKG_BATCH_SIZE = int(os.environ.get("EKG_BATCH_SIZE", "50"))

log = logging.getLogger("ekg_etl")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# SQL Registry
# ---------------------------------------------------------------------------

class SqlRegistry:
    """
    Loads SQL from queries.ini, assembles CTE preambles, and resolves
    structural placeholders.

    CTE assembly
    ────────────
    When a section declares a `ctes` key (comma-separated list of CTE names),
    get() prepends a WITH clause built from the corresponding [meta.cte.*]
    fragments before returning the assembled query.

    Structural substitutions ({schema}, {gsr_client}, {gsr_inst}) are applied
    to both the CTE fragments and the main query body.
    """

    _ALLOWED = frozenset({"schema", "gsr_client", "gsr_inst", "gsr_sdts", "java_home"})

    def __init__(self, path: Path = QUERIES_FILE) -> None:
        self._parser = configparser.ConfigParser(interpolation=None)
        if not path.exists():
            raise FileNotFoundError(f"SQL registry not found: {path}")
        self._parser.read(path, encoding="utf-8")
        self._cache: Dict[str, str] = {}
        log.debug("SqlRegistry: loaded %d sections from %s.", len(self._parser.sections()), path)

    def get(self, key: str, **subs: str) -> str:
        """
        Return the fully assembled SQL for *key* with:
          1. CTE fragments prepended as a WITH clause (when ctes= is declared)
          2. Structural placeholders substituted in both CTEs and query body

        Raises KeyError for unknown sections.
        Raises ValueError for malformed UUID substitutions.
        """
        for name in ("gsr_client", "gsr_inst"):
            if name in subs and not _UUID_RE.match(subs[name]):
                raise ValueError(
                    f"SqlRegistry: '{name}' must be a valid UUID, got {subs[name]!r}"
                )

        cache_key = key + "|" + "|".join(f"{k}={v}" for k, v in sorted(subs.items()))
        if cache_key in self._cache:
            return self._cache[cache_key]

        if not self._parser.has_section(key):
            raise KeyError(f"SqlRegistry: no section [{key}] in {QUERIES_FILE}")
        if not self._parser.has_option(key, "sql"):
            raise KeyError(f"SqlRegistry: [{key}] has no 'sql' option")

        body = self._parser.get(key, "sql")
        body = self._substitute(body, subs)
        self._cache[cache_key] = body

        # Assemble CTE preamble when the section declares cte names
        cte_names: List[str] = []
        if self._parser.has_option(key, "ctes"):
            raw = self._parser.get(key, "ctes")
            cte_names = [n.strip() for n in raw.split(",") if n.strip()]

        if cte_names:
            fragments: List[str] = []
            for name in cte_names:
                cte_key = f"meta.cte.{name}"
                if not self._parser.has_section(cte_key):
                    raise KeyError(
                        f"SqlRegistry: CTE '{name}' referenced by [{key}] "
                        f"has no section [{cte_key}]"
                    )
                fragment = self._parser.get(cte_key, "sql")
                fragment = self._substitute(fragment, subs)
                fragments.append(fragment)
            body = "WITH\n" + ",\n".join(fragments) + "\n" + body
            
        body = self._substitute(body, subs)
        self._cache[cache_key] = body
        return body

    def _substitute(self, sql: str, subs: Dict[str, str]) -> str:
        """Apply allowed structural substitutions to a SQL string."""
        for name, value in subs.items():
            if name in self._ALLOWED:
                sql = sql.replace("{" + name + "}", value)
        return sql


_sql_registry: Optional[SqlRegistry] = None


def get_sql_registry() -> SqlRegistry:
    global _sql_registry
    if _sql_registry is None:
        _sql_registry = SqlRegistry()
    return _sql_registry


# ---------------------------------------------------------------------------
# Configuration dataclasses
# ---------------------------------------------------------------------------

@dataclass
class DBConfig:
    """PostgreSQL connection parameters. No password field."""
    host:       str = "localhost"
    port:       int = 5432
    database:   str = ""
    schema:     str = "public"
    secret_ref: str = ""       # resolved via ChainedSecretsProvider


@dataclass
class TenantConfig:
    gsr_client: str
    gsr_inst:   str

    def __post_init__(self) -> None:
        for name, val in [("gsr_client", self.gsr_client), ("gsr_inst", self.gsr_inst)]:
            if not _UUID_RE.match(val):
                raise ValueError(f"TenantConfig: '{name}' must be a valid UUID, got {val!r}")


# ---------------------------------------------------------------------------
# PostgreSQL connection helper (uses ChainedSecretsProvider)
# ---------------------------------------------------------------------------

def _pg_connect(
    cfg: DBConfig, secrets: ChainedSecretsProvider
) -> psycopg2.extensions.connection:
    creds = secrets.resolve(cfg.secret_ref)
    host  = creds.get("host",   cfg.host)
    port  = int(creds.get("port", cfg.port))
    db    = creds.get("dbname", cfg.database)
    user  = creds["username"]
    pw    = creds["password"]
    log.info("Connecting to PostgreSQL: %s@%s:%s/%s", user, host, port, db)
    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw)
    conn.autocommit = False
    return conn


# ---------------------------------------------------------------------------
# Metadata model
# ---------------------------------------------------------------------------

@dataclass
class AttributeMapping:
    attr_id:          str
    source_column:    str
    target_property:  str
    data_type:        str
    nullable:         bool
    transform_expr:   Optional[str]
    is_id_component:  bool
    privacy_class:    Optional[str]
    security_class:   Optional[str]


@dataclass
class EntityMapping:
    mapping_id:           str
    table_id:             str
    entity_id:            str
    entity_name:          str
    entity_type:          str
    id_column_expr:       Optional[str]
    # Edge-only: mapping_id of the from/to node's own EntityMapping. The
    # referenced mapping is looked up by this id and its own _resolve_id()
    # is reused against this edge's row.
    from_mapping_id:      Optional[str]
    to_mapping_id:        Optional[str]
    row_filter:           Optional[str]
    change_mode:          str
    watermark_column:     Optional[str]
    delete_flag_column:   Optional[str]
    hash_columns:         Optional[List[str]]
    source_id:            str
    driver_module:        str
    host:                 Optional[str]
    port:                 Optional[int]
    database_name:        Optional[str]
    secret_ref:           str
    extra_params:         dict
    schema_name:          Optional[str]
    table_name:           str
    override_query:       Optional[str]
    partition_column:     Optional[str]
    partition_size:       int
    load_order:           int
    attributes:           List[AttributeMapping] = field(default_factory=list)
    concept_tags:         List[Tuple[str, str, str, Optional[str]]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Meta DB repository  (READ-ONLY, via temporary views)
# ---------------------------------------------------------------------------

class MetaRepository:

    def __init__(
        self,
        cfg:     DBConfig,
        tenant:  TenantConfig,
        secrets: ChainedSecretsProvider,
    ) -> None:
        self._cfg     = cfg
        self._tenant  = tenant
        self._secrets = secrets
        self._conn    = None
        self._sql     = get_sql_registry()
        self._current_sdts: Optional[datetime] = None

    def _subs(self) -> dict:
        """Structural substitutions for all meta queries."""
        subs = {
            "schema": self._cfg.schema,
            "gsr_client": self._tenant.gsr_client,
            "gsr_inst": self._tenant.gsr_inst,
            "java_home": os.environ.get("JAVA_HOME", ""),
        }
        if self._current_sdts is not None:
            subs["gsr_sdts"] = self._current_sdts.strftime("%Y-%m-%d %H:%M:%S.%f")
        return subs

    def connect(self) -> None:
        self._conn = _pg_connect(self._cfg, self._secrets)
        with self._conn.cursor() as cur:
            cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        self._conn.commit()
        log.info(
            "Meta DB connected (read-only). Tenant: client=%s inst=%s",
            self._tenant.gsr_client, self._tenant.gsr_inst,
        )

    def close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _cur(self):
        return self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)



    def get_identifier_quote_char(self, conn: jaydebeapi.Connection) -> str:
        """
        Ask the JDBC driver what identifier quote character it uses.
        Returns e.g. '"' for Postgres/Snowflake/SQL Server, '`' for MySQL.
        Returns ' ' (a single space) if the driver reports no quoting support.
        """
        metadata = conn.jconn.getMetaData()
        quote_char = metadata.getIdentifierQuoteString()
        return str(quote_char)

    def quote_ident(self, identifier: str, quote_char: str = '"') -> str:
        """
        Quote a single identifier using the given quote character,
        doubling any embedded occurrences of that character (standard
        SQL escaping rule, used by ANSI SQL / Postgres / Snowflake / SQL Server;
        MySQL follows the same doubling rule for backticks).
        """
        if quote_char.strip() == "":
            # Driver reports no quoting support - return identifier unquoted
            return identifier
        return quote_char + identifier.replace(quote_char, quote_char * 2) + quote_char

    def quote_qualified(self, *parts: str, quote_char: str = '"') -> str:
        """
        Quote a dotted, schema-qualified identifier, e.g.
        quote_qualified("my_schema", "my table") -> "my_schema"."my table"
        """
        return ".".join(self.quote_ident(p, quote_char) for p in parts)

    def load_mappings(self) -> List[EntityMapping]:
        sql = self._sql.get("meta.load_mappings", **self._subs())
        with self._cur() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        mappings = []
        for r in rows:
            m = EntityMapping(
                mapping_id           = r["mapping_id"],
                table_id             = r["table_id"],
                entity_id            = r["entity_id"],
                entity_name          = r["entity_name"],
                entity_type          = r["entity_type"],
                id_column_expr       = r["id_column_expr"],
                from_mapping_id      = r["from_mapping_id"],
                to_mapping_id        = r["to_mapping_id"],
                row_filter           = r["row_filter"],
                change_mode          = r["change_mode"],
                watermark_column     = r["watermark_column"],
                delete_flag_column   = r["delete_flag_column"],
                hash_columns         = r["hash_columns"],
                source_id            = r["source_id"],
                driver_module        = r["driver_module"],
                host                 = r["host"],
                port                 = r["port"],
                database_name        = self.quote_qualified(r["database_name"]),
                secret_ref           = r["secret_ref"],
                extra_params         = r["extra_params"] or {},
                schema_name          = self.quote_qualified(r["schema_name"]),
                table_name           = self.quote_qualified(r["table_name"]),
                override_query       = r["override_query"],
                partition_column     = r["partition_column"],
                partition_size       = r["partition_size"] or 100_000,
                load_order           = r["load_order"],
            )
            m.attributes   = self._load_attributes(m.mapping_id)
            mappings.append(m)

        tag_map = self._load_concept_tags()
        for m in mappings:
            m.concept_tags = tag_map.get(m.mapping_id, [])

        log.info(
            "Loaded %d entity mappings (client=%s inst=%s).",
            len(mappings), self._tenant.gsr_client, self._tenant.gsr_inst,
        )
        return mappings

    def _load_attributes(self, mapping_id: str) -> List[AttributeMapping]:
        sql = self._sql.get("meta.load_attributes", **self._subs())

        with self._cur() as cur:
            cur.execute(sql, (mapping_id,))
            return [
                AttributeMapping(
                    attr_id         = r["attr_id"],
                    source_column   = r["source_column"],
                    target_property = r["target_property"],
                    data_type       = r["data_type"],
                    nullable        = r["nullable"],
                    transform_expr  = r["transform_expr"],
                    is_id_component = r["is_id_component"],
                    privacy_class   = r["privacy_class"],
                    security_class  = r["security_class"],
                )
                for r in cur.fetchall()
            ]

    def _load_concept_tags(self) -> Dict[str, List[Tuple[str, str, str, Optional[str]]]]:
        sql = self._sql.get("meta.load_concept_tags", **self._subs())
        result: Dict[str, List[Tuple[str, str, str, Optional[str]]]] = {}
        with self._cur() as cur:
            cur.execute(sql)
            for r in cur.fetchall():
                result.setdefault(r["mapping_id"], []).append(
                    (r["tag_id"], r["tag_name"], r["tag_category"], r["display_name"])
                )
        log.info("Loaded %d concept tag assignments.", sum(len(v) for v in result.values()))
        return result

    def load_current_sdts(self) -> datetime:
        """
        Fetch the active snapshot timestamp (gsr_sdts) for the current tenant.

        Called once at pipeline startup. The timestamp is stored on this repository
        so later meta queries can substitute {gsr_sdts} safely.
        """
        sql = self._sql.get("meta.load_current_sdts", **self._subs())
        with self._cur() as cur:
            cur.execute(sql)
            row = cur.fetchone()

        if row is None:
            raise RuntimeError(
                f"No active control_sdts row found for "
                f"gsr_client={self._tenant.gsr_client} "
                f"gsr_inst={self._tenant.gsr_inst}. "
                f"Ensure exactly one active control_sdts row exists for this tenant."
            )

        sdts: datetime = row["gsr_sdts"]
        self._current_sdts = sdts

        log.info(
            "Current snapshot timestamp (gsr_sdts): %s  "
            "(client=%s inst=%s)",
            sdts, self._tenant.gsr_client, self._tenant.gsr_inst,
        )
        return sdts


# ---------------------------------------------------------------------------
# Run DB repository  (READ-WRITE)
# ---------------------------------------------------------------------------

class RunRepository:

    def __init__(self, cfg: DBConfig, secrets: ChainedSecretsProvider) -> None:
        self._cfg     = cfg
        self._secrets = secrets
        self._conn    = None
        self._sql     = get_sql_registry()

    def _subs(self) -> dict:
        return {"schema": self._cfg.schema}

    def connect(self) -> None:
        self._conn = _pg_connect(self._cfg, self._secrets)

    def close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _cur(self):
        return self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def commit(self) -> None:
        self._conn.commit()

    def start_run(self, run_type: str, s3_prefix: str, triggered_by: str) -> str:
        run_id = str(uuid.uuid4())
        with self._cur() as cur:
            cur.execute(
                self._sql.get("run.start_run", **self._subs()),
                (run_id, run_type, _utcnow(), s3_prefix or None, triggered_by),
            )
        self.commit()
        log.info("Started %s run %s.", run_type, run_id)
        return run_id

    def finish_run(
        self,
        run_id: str,
        status: str,
        bulk_load_job_id: str = "",
        rows_upserted: int = 0,
        rows_deleted: int = 0,
        rows_skipped: int = 0,
        error_message: str = "",
    ) -> None:
        with self._cur() as cur:
            cur.execute(
                self._sql.get("run.finish_run", **self._subs()),
                (
                    _utcnow(), status, bulk_load_job_id or None,
                    rows_upserted, rows_deleted, rows_skipped,
                    error_message or None, run_id,
                ),
            )
        self.commit()
        log.info("Run %s → %s  upserted=%d  deleted=%d  skipped=%d",
                 run_id, status, rows_upserted, rows_deleted, rows_skipped)

    def record_detail(
        self, run_id: str, mapping_id: str, operation: str,
        s3_uri: str = "", rows_upserted: int = 0, rows_deleted: int = 0,
        rows_skipped: int = 0, error_message: str = "",
    ) -> None:
        with self._cur() as cur:
            cur.execute(
                self._sql.get("run.record_detail", **self._subs()),
                (run_id, mapping_id, operation, s3_uri or None,
                 rows_upserted, rows_deleted, rows_skipped,
                 error_message or None, _utcnow(), _utcnow()),
            )
        self.commit()

    def get_watermark(self, mapping_id: str) -> Optional[datetime]:
        with self._cur() as cur:
            cur.execute(self._sql.get("run.get_watermark", **self._subs()), (mapping_id,))
            row = cur.fetchone()
        return row["last_watermark"] if row else None

    def set_pending_watermark(self, mapping_id: str, pending: datetime, run_id: str) -> None:
        with self._cur() as cur:
            cur.execute(
                self._sql.get("run.set_pending_watermark", **self._subs()),
                (mapping_id, pending, run_id, _utcnow()),
            )
        self.commit()

    def commit_watermark(self, mapping_id: str) -> None:
        with self._cur() as cur:
            cur.execute(
                self._sql.get("run.commit_watermark", **self._subs()),
                (_utcnow(), mapping_id),
            )
        self.commit()

    def load_hashes(self, mapping_id: str) -> Dict[str, str]:
        with self._cur() as cur:
            cur.execute(self._sql.get("run.load_hashes", **self._subs()), (mapping_id,))
            return {r["entity_id_value"]: r["row_hash"] for r in cur.fetchall()}

    def upsert_hashes(self, mapping_id: str, run_id: str, hashes: Dict[str, str]) -> None:
        if not hashes:
            return
        now = _utcnow()
        with self._cur() as cur:
            psycopg2.extras.execute_values(
                cur,
                self._sql.get("run.upsert_hashes", **self._subs()),
                [(mapping_id, eid, h, run_id, run_id, now) for eid, h in hashes.items()],
                template=self._sql.get("run.upsert_hashes_template", **self._subs()),
            )
        self.commit()

    def delete_hashes(self, mapping_id: str, entity_ids: List[str]) -> None:
        if not entity_ids:
            return
        with self._cur() as cur:
            cur.execute(
                self._sql.get("run.delete_hashes", **self._subs()),
                (mapping_id, entity_ids),
            )
        self.commit()

    def clear_mapping_state(self, mapping_ids: List[str]) -> None:
        if not mapping_ids:
            return
        with self._cur() as cur:
            cur.execute(self._sql.get("run.clear_hashes",     **self._subs()), (mapping_ids,))
            cur.execute(self._sql.get("run.clear_watermarks", **self._subs()), (mapping_ids,))
        self.commit()
        log.info("Cleared incremental state for %d mappings.", len(mapping_ids))


# ---------------------------------------------------------------------------
# Source database connections  (generic DB-API 2.0)
# ---------------------------------------------------------------------------

class SourceConnectionFactory:

    def __init__(self, secrets: ChainedSecretsProvider) -> None:
        self._secrets = secrets
        self._cache: Dict[str, Any] = {}

    @staticmethod
    def _resolve_jvm_path(extra_params: Dict[str, Any]) -> Optional[str]:
        """
        Resolve a JPype-compatible JVM shared library path.

        JayDeBeApi uses JPype under the hood. JPype needs the actual JVM library
        file, not just JAVA_HOME. On macOS that is usually:
            <JAVA_HOME>/lib/server/libjvm.dylib
        """
        configured = (
            extra_params.get("jvm_path")
            or os.environ.get("JPYPE_JVM")
            or os.environ.get("JVM_PATH")
        )

        if configured:
            path = Path(str(configured)).expanduser()
            if path.is_dir():
                candidate = path / "lib" / "server" / "libjvm.dylib"
                if candidate.exists():
                    return str(candidate)
            if path.exists():
                return str(path)
            raise RuntimeError(
                "Configured JVM path does not exist: "
                f"{path}. Set JPYPE_JVM/JVM_PATH to the actual libjvm.dylib file, "
                "or set JAVA_HOME to a valid JDK home."
            )

        java_home = extra_params.get("java_home") or os.environ.get("JAVA_HOME")
        if java_home:
            home = Path(str(java_home)).expanduser()
            candidates = [
                home / "lib" / "server" / "libjvm.dylib",
                home / "jre" / "lib" / "server" / "libjvm.dylib",
            ]
            for candidate in candidates:
                if candidate.exists():
                    return str(candidate)

            raise RuntimeError(
                "JAVA_HOME is set, but the JVM library was not found. "
                f"JAVA_HOME={home}. Expected a file like "
                f"{home}/lib/server/libjvm.dylib."
            )

        return None

    def get_connection(self, m: EntityMapping) -> Any:
        if m.source_id in self._cache:
            try:
                self._cache[m.source_id].cursor().close()
                return self._cache[m.source_id]
            except Exception:
                log.warning("Stale connection for source %s; reconnecting.", m.source_id)

        creds = self._secrets.resolve(m.secret_ref)
        host  = creds.get("host",   m.host)
        port  = int(creds.get("port", m.port or 5432))
        db    = creds.get("dbname", m.database_name)
        user  = creds["username"]
        pw    = creds["password"]

        driver = importlib.import_module(m.driver_module)
        log.info("Opening %s → %s@%s:%s/%s", m.driver_module, user, host, port, db)

        if driver.__name__ == "jaydebeapi":
            jdbc_driver_class = m.extra_params.get("jdbc_driver_class")
            jdbc_url = m.extra_params.get("jdbc_url")
            jdbc_jar_path = m.extra_params.get("jdbc_jar_path")

            if not jdbc_driver_class or not jdbc_url or not jdbc_jar_path:
                raise RuntimeError(
                    "JayDeBeApi source requires extra_params containing "
                    "'jdbc_driver_class', 'jdbc_url', and 'jdbc_jar_path'."
                )

            jdbc_jar = Path(str(jdbc_jar_path)).expanduser()
            if not jdbc_jar.exists():
                raise RuntimeError(
                    f"JDBC driver JAR not found: {jdbc_jar}. "
                    "Check extra_params['jdbc_jar_path'] for this data source."
                )

            jvm_path = self._resolve_jvm_path(m.extra_params)

            if jvm_path:
                conn = driver.connect(
                    jdbc_driver_class,
                    jdbc_url,
                    [user, pw],
                    str(jdbc_jar),
                    jvm_path=jvm_path,
                )
            else:
                conn = driver.connect(
                    jdbc_driver_class,
                    jdbc_url,
                    [user, pw],
                    str(jdbc_jar),
                )
        else:
            conn = driver.connect(
                host=host, port=port, database=db, user=user, password=pw, **m.extra_params
            )

        self._cache[m.source_id] = conn
        return conn

    def close_all(self) -> None:
        for sid, conn in self._cache.items():
            try:
                conn.close()
            except Exception as exc:
                log.warning("Error closing source %s: %s", sid, exc)
        self._cache.clear()

    def build_jdbc_classpath(self, jar_folder: str) -> str:
        """
        Scan `jar_folder` for .jar files and return them joined into a single
        classpath string, using the OS-appropriate path separator
        (';' on Windows, ':' on Linux/macOS).

        Parameters
        ----------
        jar_folder : str
            Path to the folder containing the JDBC driver jar(s).

        Returns
        -------
        str
            Classpath string, e.g. "/path/jars/driver1.jar:/path/jars/driver2.jar"

        Raises
        ------
        FileNotFoundError
            If the folder does not exist.
        ValueError
            If no .jar files are found in the folder.
        """
        if not os.path.isdir(jar_folder):
            raise FileNotFoundError(f"Jar folder not found: {jar_folder}")

        jar_files = sorted(glob.glob(os.path.join(jar_folder, "*.jar")))

        if not jar_files:
            raise ValueError(f"No .jar files found in: {jar_folder}")

        # Normalize to absolute paths for reliability regardless of CWD
        jar_files = [os.path.abspath(jar) for jar in jar_files]

        classpath = os.pathsep.join(jar_files)
        return classpath


    def get_connection(self, m: EntityMapping) -> Any:
        if m.source_id in self._cache:
            try:
                self._cache[m.source_id].cursor().close()
                return self._cache[m.source_id]
            except Exception:
                log.warning("Stale connection for source %s; reconnecting.", m.source_id)

        creds = self._secrets.resolve(m.secret_ref)
        host  = creds.get("host",   m.host)
        port  = int(creds.get("port", m.port or 5432))
        db    = creds.get("dbname", m.database_name)
        user  = creds["username"]
        pw    = creds["password"]

        driver = importlib.import_module(m.driver_module)
        log.info("Opening %s → %s@%s:%s/%s", m.driver_module, user, host, port, db)

        if driver.__name__ == "jaydebeapi":
            jdbc_driver_class = m.extra_params.get("jdbc_driver_class")
            jdbc_url = m.extra_params.get("jdbc_url")
            jdbc_jar_path = self.build_jdbc_classpath("jdbc")
            #'jdbc/postgresql-42.7.13.jar;jdbc/snowflake-jdbc-4.3.1.jar' #m.extra_params.get("jdbc_jar_path")

            log.info('Java Home %s', os.environ.get("JAVA_HOME", ""))
            log.info('JDBC Driver Class %s', jdbc_driver_class)
            log.info('JDBC URL %s', jdbc_url)
            log.info('JDBC JAR Path %s', jdbc_jar_path)

            if not jdbc_driver_class or not jdbc_url or not jdbc_jar_path:
                raise RuntimeError(
                    "JayDeBeApi source requires extra_params containing "
                    "'jdbc_driver_class', 'jdbc_url', and 'jdbc_jar_path'."
                )

            conn = driver.connect(
                jdbc_driver_class,
                jdbc_url,
                [user, pw],
                jdbc_jar_path,
            )
        else:
            conn = driver.connect(
                host=host, port=port, database=db, user=user, password=pw, **m.extra_params
            )

        self._cache[m.source_id] = conn
        return conn

    def close_all(self) -> None:
        for sid, conn in self._cache.items():
            try:
                conn.close()
            except Exception as exc:
                log.warning("Error closing source %s: %s", sid, exc)
        self._cache.clear()


# ---------------------------------------------------------------------------
# Source extraction helpers
# ---------------------------------------------------------------------------

def _base_query(m: EntityMapping) -> str:
    if m.override_query:
        q = m.override_query
    else:
        qual = f"{m.schema_name}.{m.table_name}" if m.schema_name else m.table_name
        q = f"SELECT * FROM {qual}"
    if m.row_filter:
        q = f"SELECT * FROM ({q}) AS _src WHERE {m.row_filter}"
    return q


def _normalise_row(raw_cols: List[str], raw_row: tuple) -> Dict[str, Any]:
    """
    Build a normalised row dict from a raw DB cursor row.

    Two transformations are applied:
      1. Column names are lowercased so expressions are case-insensitive.
         "Customer ID", "CUSTOMER_ID", and "customer_id" all become
         "customer_id" in the row dict.
      2. The dict is added to the eval scope under the key 'row' so that
         columns with spaces or other characters that are invalid Python
         identifiers can be accessed via row['column name'] in expressions.

    The resulting dict therefore supports both styles:
      id_column_expr = "'Cust_' + str(customer_id)"          # bare name
      id_column_expr = "'Cust_' + str(row['customer id'])"   # dict access
    """
    normalised: Dict[str, Any] = {
        col.lower(): val for col, val in zip(raw_cols, raw_row)
    }
    # Expose the dict itself under 'row' for expressions that need
    # dict-style access (spaces, hyphens, reserved words in column names)
    normalised["row"] = normalised.copy()
    return normalised


def _iter_source(
        conn: Any, m: EntityMapping, extra_where: str = ""
) -> Iterator[List[Dict[str, Any]]]:
    base = _base_query(m)

    def _with_extra(q: str) -> str:
        return f"SELECT * FROM ({q}) AS _ew WHERE {extra_where}" if extra_where else q

    cur = conn.cursor()
    if m.partition_column:
        bounds_q = (
            f"SELECT MIN({m.partition_column}), MAX({m.partition_column}) "
            f"FROM ({_with_extra(base)}) AS _b"
        )
        cur.execute(bounds_q)
        min_val, max_val = cur.fetchone()
        if min_val is None:
            cur.close()
            return
        log.debug("Partition %s on %s [%s..%s] chunk=%d",
                  m.table_name, m.partition_column, min_val, max_val, m.partition_size)
        lo = min_val
        while lo <= max_val:
            hi = lo + m.partition_size - 1
            pq = (f"SELECT * FROM ({_with_extra(base)}) AS _p "
                  f"WHERE {m.partition_column} BETWEEN {lo!r} AND {hi!r}")
            cur.execute(pq)
            raw_cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            if rows:
                yield [_normalise_row(raw_cols, row) for row in rows]
            lo = hi + 1
    else:
        cur.execute(_with_extra(base))
        raw_cols = [d[0] for d in cur.description]
        while True:
            rows = cur.fetchmany(m.partition_size)
            if not rows:
                break
            yield [_normalise_row(raw_cols, row) for row in rows]
    cur.close()


# ---------------------------------------------------------------------------
# Value helpers
# ---------------------------------------------------------------------------

_SAFE_BUILTINS: dict = {"__builtins__": {}}


def _eval_expr(expr: str, row: dict) -> Any:
    return eval(expr, _SAFE_BUILTINS, row)  # noqa: S307


def _apply_transform(value: Any, expr: Optional[str]) -> Any:
    if not expr:
        return value
    try:
        return eval(expr, _SAFE_BUILTINS, {"value": value})  # noqa: S307
    except Exception as exc:
        log.debug("Transform '%s' failed on %r: %s", expr, value, exc)
        return value


def _format_csv_value(value: Any, data_type: str) -> str:
    if value is None:
        return ""
    if data_type == "Bool":
        return "true" if value else "false"
    if data_type in ("Date", "DateTime"):
        return value.isoformat() if hasattr(value, "isoformat") else str(value)
    return str(value)


def _compute_hash(row: dict, columns: List[str]) -> str:
    h = hashlib.sha256()
    for col in columns:
        v = row.get(col)
        h.update(str(v).encode() if v is not None else b"\x00")
    return h.hexdigest()


def _resolve_id(m: EntityMapping, row: dict) -> Optional[str]:
    """
    Produce a stable Neptune ~id string for a source row.

    Resolution order
    ────────────────
    1. id_column_expr is set  →  evaluate the Python expression against the row.
    2. id_column_expr is NULL →  composite key: concatenate entity_name with the
       lowercased values of all attribute columns where is_id_component = TRUE,
       joined by '_', in attr_id order.
       Example: entity "Customer", id components [customer_id=42, region="DE"]
                → "Customer_42_DE"
    3. No is_id_component attrs → hash fallback: SHA-256 of entity_name + all
       mapped column values in attr_id order, prefixed with entity_name.
       Example: "Customer_a3f2c1d4..."
       Deterministic for the same row content; will NOT survive column reordering
       or value changes (row will be treated as a new node).
    4. No attributes at all    →  log an error and return None (row is skipped).
    """
    # Strategy 1: explicit expression
    if m.id_column_expr:
        try:
            return str(_eval_expr(m.id_column_expr, row))
        except Exception as exc:
            log.warning(
                "id_column_expr eval failed for mapping %s: %s  row keys=%s",
                m.mapping_id, exc, list(row.keys()),
            )
            return None

    # Strategy 2: composite key from is_id_component attributes
    id_attrs = [a for a in m.attributes if a.is_id_component]
    if id_attrs:
        parts = [m.entity_name]
        for attr in id_attrs:
            col_key = attr.source_column.lower()
            val = row.get(col_key)
            if val is None:
                val = ''
            ##    log.warning(
            ##        "Composite id: column '%s' is NULL in mapping %s — row skipped.",
            ##        attr.source_column, m.mapping_id,
            ##    )
            ##    return None
            parts.append(str(val))
        return "_".join(parts)

    # Strategy 3: hash of all mapped column values
    all_attrs = [a for a in m.attributes]
    if all_attrs:
        h = hashlib.sha256()
        h.update(m.entity_name.encode())
        for attr in all_attrs:
            col_key = attr.source_column.lower()
            val = row.get(col_key)
            h.update(b"\x1f")  # field separator (ASCII unit separator)
            h.update(str(val).encode() if val is not None else b"\x00")
        node_id = f"{m.entity_name}_{h.hexdigest()}"
        log.debug(
            "Hash fallback id for mapping %s: %s  "
            "(no id_column_expr and no is_id_component attributes defined)",
            m.mapping_id, node_id,
        )
        return node_id

    # Strategy 4: nothing available
    log.error(
        "Mapping %s (%s): cannot generate ~id — "
        "id_column_expr is NULL, no is_id_component attributes, "
        "and no mapped attributes at all.",
        m.mapping_id, m.entity_name,
    )
    return None


def _resolve_ref_id(
    mapping_by_id: Dict[str, EntityMapping],
    ref_mapping_id: Optional[str],
    row: dict,
    mapping_id: str,
    side: str,
) -> Optional[str]:
    """
    Resolve an edge's 'from' or 'to' endpoint ~id from the edge's own row.

    from_mapping_id / to_mapping_id holds the mapping_id of the referenced
    node's own EntityMapping (see meta.cte.entity_mapping's edge branch,
    which joins to the referenced hub/link and selects its mapping_id). Look
    that mapping up and reuse its own _resolve_id() against this edge's row
    — the row must expose the same column names the referenced node's own
    id resolution reads.
    """
    if not ref_mapping_id:
        log.error(
            "Mapping %s: %s_mapping_id is not set — cannot resolve %s-side id.",
            mapping_id, side, side,
        )
        return None

    target = mapping_by_id.get(ref_mapping_id)
    if target is None:
        log.error(
            "Mapping %s: %s_mapping_id '%s' does not match any loaded mapping_id.",
            mapping_id, side, ref_mapping_id,
        )
        return None

    return _resolve_id(target, row)


def _hash_columns_for(m: EntityMapping) -> List[str]:
    return m.hash_columns or [a.source_column for a in m.attributes]


def _row_to_props(m: EntityMapping, row: dict) -> Dict[str, Any]:
    props: Dict[str, Any] = {}
    for attr in m.attributes:
        # source_column may have been entered in any casing in the metadata;
        # normalise to lowercase to match the normalised row dict keys.
        col_key = attr.source_column.lower()
        val = _apply_transform(row.get(col_key), attr.transform_expr)
        props[attr.target_property] = val
        if attr.privacy_class:
            props[f"{attr.target_property}__privacy_class"] = attr.privacy_class
        if attr.security_class:
            props[f"{attr.target_property}__security_class"] = attr.security_class
    return props


# ---------------------------------------------------------------------------
# Change-detection
# ---------------------------------------------------------------------------

class _ChangeResult:
    __slots__ = ("upsert_nodes", "upsert_edges", "delete_ids", "skipped", "new_hashes", "seen_ids")

    def __init__(self) -> None:
        self.upsert_nodes: List[Tuple[str, str, Dict[str, Any]]]           = []
        self.upsert_edges: List[Tuple[str, str, str, str, Dict[str, Any]]] = []
        self.delete_ids:   List[str]                                        = []
        self.skipped:      int                                              = 0
        self.new_hashes:   Dict[str, str]                                   = {}
        self.seen_ids:     Set[str]                                         = set()


def _detect_timestamp(
    m: EntityMapping,
    batches: Iterator[List[dict]],
    mapping_by_id: Dict[str, EntityMapping],
) -> _ChangeResult:
    result = _ChangeResult()
    for batch in batches:
        for row in batch:
            node_id = _resolve_id(m, row)
            if node_id is None:
                result.skipped += 1
                continue
            if m.delete_flag_column:
                try:
                    flag = _eval_expr(m.delete_flag_column, row)
                except Exception:
                    flag = row.get(m.delete_flag_column)
                if flag:
                    result.delete_ids.append(node_id)
                    continue
            props = _row_to_props(m, row)
            if m.entity_type == "node":
                result.upsert_nodes.append((node_id, m.entity_name, props))
            else:
                from_id = _resolve_ref_id(
                    mapping_by_id, m.from_mapping_id,
                    row, m.mapping_id, "from",
                )
                to_id = _resolve_ref_id(
                    mapping_by_id, m.to_mapping_id,
                    row, m.mapping_id, "to",
                )
                if from_id is None or to_id is None:
                    result.skipped += 1
                    continue
                result.upsert_edges.append((node_id, m.entity_name, from_id, to_id, props))
    return result


def _detect_hash(
    m: EntityMapping,
    batches: Iterator[List[dict]],
    stored: Dict[str, str],
    mapping_by_id: Dict[str, EntityMapping],
) -> _ChangeResult:
    result        = _ChangeResult()
    hash_cols     = _hash_columns_for(m)
    always_upsert = (m.change_mode == "full")

    for batch in batches:
        for row in batch:
            node_id = _resolve_id(m, row)
            if node_id is None:
                result.skipped += 1
                continue
            current_hash = _compute_hash(row, hash_cols)
            result.seen_ids.add(node_id)
            result.new_hashes[node_id] = current_hash
            if not always_upsert and stored.get(node_id) == current_hash:
                continue
            props = _row_to_props(m, row)
            if m.entity_type == "node":
                result.upsert_nodes.append((node_id, m.entity_name, props))
            else:
                from_id = _resolve_ref_id(
                    mapping_by_id, m.from_mapping_id,
                    row, m.mapping_id, "from",
                )
                to_id = _resolve_ref_id(
                    mapping_by_id, m.to_mapping_id,
                    row, m.mapping_id, "to",
                )
                if from_id is None or to_id is None:
                    result.skipped += 1
                    continue
                result.upsert_edges.append((node_id, m.entity_name, from_id, to_id, props))

    result.delete_ids = list(set(stored.keys()) - result.seen_ids)
    return result


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

class EKGETLPipeline:
    """
    Enterprise Knowledge Graph ETL pipeline.

    Graph-neutral: all writes go through the GraphTarget ABC.
    Credential-neutral: all resolution goes through ChainedSecretsProvider.
    Logging-neutral: handlers are built by configure_logging() before pipeline
    construction.

    Parameters
    ----------
    meta_cfg      DBConfig for the read-only meta database.
    run_cfg       DBConfig for the read-write run database.
    target        GraphTarget instance (from graph.create_graph_target).
    tenant        TenantConfig with gsr_client and gsr_inst UUIDs.
    secrets       ChainedSecretsProvider for all credential resolution.
    bulk_staging  Optional staging location string passed to run audit.
    """

    def __init__(
        self,
        meta_cfg:     DBConfig,
        run_cfg:      DBConfig,
        target:       GraphTarget,
        tenant:       TenantConfig,
        secrets:      ChainedSecretsProvider,
        bulk_staging: str = "",
    ) -> None:
        self._target      = target
        self._tenant      = tenant
        self._bulk_staging = bulk_staging
        self._meta_repo   = MetaRepository(meta_cfg, tenant, secrets)
        self._run_repo    = RunRepository(run_cfg, secrets)
        self._src_factory = SourceConnectionFactory(secrets)
        self._current_sdts: Optional[datetime] = None   # set at connect time
        self._mapping_by_id: Dict[str, EntityMapping] = {}

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------

    def run_full(self, triggered_by: str = "pipeline") -> None:
        """Initial / full reload using each target's bulk-load mechanism."""
        self._target.precheck()
        self._meta_repo.connect()
        self._run_repo.connect()
        self._current_sdts = self._meta_repo.load_current_sdts()
        run_id   = self._run_repo.start_run("full", self._bulk_staging, triggered_by)
        total_up = total_sk = 0
        job_id   = ""
        status   = "SUCCESS"

        try:
            mappings = self._meta_repo.load_mappings()
            self._mapping_by_id = {mm.mapping_id: mm for mm in mappings}
            self._target.begin_bulk()

            # Write ConceptTag nodes first (before data nodes)
            self._bulk_concept_tags(mappings)

            for m in mappings:
                log.info("Full load mapping %s: %s (%s)", m.mapping_id, m.entity_name, m.entity_type)
                try:
                    conn    = self._src_factory.get_connection(m)
                    written = skipped = 0
                    for batch in _iter_source(conn, m):
                        for row in batch:
                            rec = self._row_to_bulk_record(m, row)
                            if rec is None:
                                skipped += 1
                                continue
                            if m.entity_type == "node":
                                self._target.write_bulk_node(rec)
                            else:
                                self._target.write_bulk_edge(rec)
                                ##log.info("Edge %s (%s): %s -> %s",
                                ##         rec.entity_id, rec.label, rec.from_id, rec.to_id)
                            written += 1
                    total_up += written
                    total_sk += skipped
                    self._run_repo.record_detail(
                        run_id, m.mapping_id, "bulk_load",
                        rows_upserted=written, rows_skipped=skipped,
                    )
                    log.info("Mapping %s: %d rows buffered, %d skipped.", m.mapping_id, written, skipped)
                except Exception as exc:
                    log.exception("Mapping %s failed: %s", m.mapping_id, exc)
                    self._run_repo.record_detail(
                        run_id, m.mapping_id, "bulk_load", error_message=str(exc)
                    )
                    status = "PARTIAL"

            job_id = self._target.commit_bulk()
            if not job_id and status == "SUCCESS":
                pass   # targets without job IDs (Neo4j, Cosmos, Spanner) return ""
            self._run_repo.clear_mapping_state([m.mapping_id for m in mappings])

        except Exception as exc:
            log.exception("Full run failed: %s", exc)
            status = "FAILED"
        finally:
            self._run_repo.finish_run(
                run_id, status,
                bulk_load_job_id=job_id,
                rows_upserted=total_up,
                rows_skipped=total_sk,
            )
            self._src_factory.close_all()
            self._meta_repo.close()
            self._run_repo.close()

    def run_incremental(self, triggered_by: str = "pipeline") -> None:
        """Incremental run: change detection + graph upserts / hard deletes."""
        self._target.precheck()
        self._meta_repo.connect()
        self._run_repo.connect()
        self._current_sdts = self._meta_repo.load_current_sdts()
        run_id    = self._run_repo.start_run("incremental", "", triggered_by)
        run_start = _utcnow()
        total_up = total_del = total_sk = 0
        status = "SUCCESS"

        try:
            mappings = self._meta_repo.load_mappings()
            self._mapping_by_id = {mm.mapping_id: mm for mm in mappings}
            self._ensure_concept_tag_nodes(mappings)

            for m in mappings:
                log.info("Incremental mapping %s: %s (%s) mode=%s",
                         m.mapping_id, m.entity_name, m.entity_type, m.change_mode)
                try:
                    up, deleted, sk = self._process_mapping(m, run_id, run_start)
                    total_up  += up
                    total_del += deleted
                    total_sk  += sk
                except Exception as exc:
                    log.exception("Mapping %s failed: %s", m.mapping_id, exc)
                    self._run_repo.record_detail(
                        run_id, m.mapping_id, "graph_upsert", error_message=str(exc)
                    )
                    status = "PARTIAL"

        except Exception as exc:
            log.exception("Incremental run failed: %s", exc)
            status = "FAILED"
        finally:
            self._run_repo.finish_run(
                run_id, status,
                rows_upserted=total_up, rows_deleted=total_del, rows_skipped=total_sk,
            )
            self._src_factory.close_all()
            self._meta_repo.close()
            self._run_repo.close()
            self._target.close()

    # ------------------------------------------------------------------
    # Concept tag helpers
    # ------------------------------------------------------------------

    def _all_unique_tags(
        self, mappings: List[EntityMapping]
    ) -> Dict[str, Tuple[str, str, str, Optional[str]]]:
        seen: Dict[str, Tuple] = {}
        for m in mappings:
            if m.entity_type != "node":
                continue
            for tag_id, tag_name, tag_category, display_name in m.concept_tags:
                if tag_id not in seen:
                    seen[tag_id] = (tag_id, tag_name, tag_category, display_name)
        return seen

    def _bulk_concept_tags(self, mappings: List[EntityMapping]) -> None:
        for tag_id, tag_name, tag_category, display_name in self._all_unique_tags(mappings).values():
            rec = BulkRecord(
                entity_type = "node",
                entity_id   = tag_id,
                label       = "ConceptTag",
                props       = {
                    "name":         tag_name,
                    "category":     tag_category,
                    "display_name": display_name or tag_name,
                },
            )
            self._target.write_bulk_node(rec)
        log.info("Bulk-buffered %d ConceptTag nodes.", len(self._all_unique_tags(mappings)))

    def _ensure_concept_tag_nodes(self, mappings: List[EntityMapping]) -> None:
        for tag_id, tag_name, tag_category, display_name in self._all_unique_tags(mappings).values():
            self._target.upsert_concept_tag(tag_id, tag_name, tag_category, display_name)
        n = len(self._all_unique_tags(mappings))
        if n:
            log.info("Ensured %d ConceptTag node(s) in graph.", n)

    def _apply_tagged_as_edges(self, m: EntityMapping, node_ids: List[str]) -> int:
        if not m.concept_tags or not node_ids:
            return 0
        count = 0
        for node_id in node_ids:
            for tag_id, _, _, _ in m.concept_tags:
                self._target.upsert_tagged_as(node_id, tag_id)
                count += 1
        log.debug("Wrote %d TAGGED_AS edge(s) for mapping %s.", count, m.mapping_id)
        return count

    # ------------------------------------------------------------------
    # Bulk record builder
    # ------------------------------------------------------------------

    def _row_to_bulk_record(
        self, m: EntityMapping, row: dict
    ) -> Optional[BulkRecord]:
        node_id = _resolve_id(m, row)
        if node_id is None:
            return None
        props = _row_to_props(m, row)
        if m.entity_type == "node":
            return BulkRecord(
                entity_type = "node",
                entity_id   = node_id,
                label       = m.entity_name,
                props       = props,
            )
        from_id = _resolve_ref_id(
            self._mapping_by_id, m.from_mapping_id,
            row, m.mapping_id, "from",
        )
        to_id = _resolve_ref_id(
            self._mapping_by_id, m.to_mapping_id,
            row, m.mapping_id, "to",
        )
        if from_id is None or to_id is None:
            return None
        return BulkRecord(
            entity_type = "edge",
            entity_id   = node_id,
            label       = m.entity_name,
            props       = props,
            from_id     = from_id,
            to_id       = to_id,
        )

    # ------------------------------------------------------------------
    # Per-mapping incremental processing
    # ------------------------------------------------------------------

    def _process_mapping(
        self, m: EntityMapping, run_id: str, run_start: datetime
    ) -> Tuple[int, int, int]:
        conn = self._src_factory.get_connection(m)
        if m.change_mode == "timestamp":
            return self._timestamp_mode(m, conn, run_id, run_start)
        else:
            return self._hash_mode(m, conn, run_id)

    def _timestamp_mode(
        self, m: EntityMapping, conn: Any, run_id: str, run_start: datetime
    ) -> Tuple[int, int, int]:
        last_wm = self._run_repo.get_watermark(m.mapping_id)
        self._run_repo.set_pending_watermark(m.mapping_id, run_start, run_id)
        extra_where = (
            f"{m.watermark_column} > '{last_wm.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
            if last_wm is not None else ""
        )
        log.info("Timestamp mode mapping %s: last_watermark=%s", m.mapping_id, last_wm)
        result  = _detect_timestamp(
            m, _iter_source(conn, m, extra_where=extra_where), self._mapping_by_id,
        )
        up      = self._apply_upserts(m, result)
        deleted = self._apply_deletes(m, result.delete_ids)
        self._run_repo.commit_watermark(m.mapping_id)
        self._run_repo.record_detail(
            run_id, m.mapping_id, "graph_upsert",
            rows_upserted=up, rows_deleted=deleted, rows_skipped=result.skipped,
        )
        return up, deleted, result.skipped

    def _hash_mode(
        self, m: EntityMapping, conn: Any, run_id: str
    ) -> Tuple[int, int, int]:
        stored  = self._run_repo.load_hashes(m.mapping_id)
        log.info("Hash mode mapping %s: %d stored hashes.", m.mapping_id, len(stored))
        result  = _detect_hash(m, _iter_source(conn, m), stored, self._mapping_by_id)
        up      = self._apply_upserts(m, result)
        deleted = self._apply_deletes(m, result.delete_ids)
        self._run_repo.upsert_hashes(m.mapping_id, run_id, result.new_hashes)
        self._run_repo.delete_hashes(m.mapping_id, result.delete_ids)
        self._run_repo.record_detail(
            run_id, m.mapping_id, "graph_upsert",
            rows_upserted=up, rows_deleted=deleted, rows_skipped=result.skipped,
        )
        return up, deleted, result.skipped

    def _apply_upserts(self, m: EntityMapping, result: _ChangeResult) -> int:
        total = 0
        if m.entity_type == "node":
            upserted_ids: List[str] = []
            for i in range(0, len(result.upsert_nodes), EKG_BATCH_SIZE):
                for node_id, label, props in result.upsert_nodes[i : i + EKG_BATCH_SIZE]:
                    self._target.upsert_node(node_id, label, props)
                    upserted_ids.append(node_id)
                    total += 1
                log.debug("Upserted %d/%d nodes (mapping %s).",
                           total, len(result.upsert_nodes), m.mapping_id)
            self._apply_tagged_as_edges(m, upserted_ids)
        else:
            for i in range(0, len(result.upsert_edges), EKG_BATCH_SIZE):
                for edge_id, label, from_id, to_id, props in result.upsert_edges[i : i + EKG_BATCH_SIZE]:
                    self._target.upsert_edge(edge_id, label, from_id, to_id, props)
                    ##log.info("Edge %s (%s): %s -> %s", edge_id, label, from_id, to_id)
                    total += 1
                log.debug("Upserted %d/%d edges (mapping %s).",
                           total, len(result.upsert_edges), m.mapping_id)
        return total

    def _apply_deletes(self, m: EntityMapping, ids: List[str]) -> int:
        if not ids:
            return 0
        log.info("Hard-deleting %d %s(s) (mapping %s).", len(ids), m.entity_type, m.mapping_id)
        for i in range(0, len(ids), EKG_BATCH_SIZE):
            for eid in ids[i : i + EKG_BATCH_SIZE]:
                if m.entity_type == "node":
                    self._target.delete_vertex(eid)
                else:
                    self._target.delete_edge(eid)
        return len(ids)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="EKG ETL pipeline  (v5)")
    parser.add_argument("--mode",       choices=["full", "incremental"], default="incremental")
    parser.add_argument("--no-console", dest="console",    action="store_false", default=True)
    parser.add_argument("--debug",      dest="debug",      action="store_true",  default=False)
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # 1. Logging
    # ------------------------------------------------------------------
    log_targets = [t.strip() for t in os.environ.get("EKG_LOG_TARGETS", "console").split(",")]
    configure_logging(
        targets    = log_targets,
        log_level  = logging.DEBUG if args.debug else logging.INFO,
        log_group  = os.environ.get("EKG_LOG_GROUP",  "/ekg-etl"),
        log_stream = os.environ.get("EKG_LOG_STREAM", "ekg-etl"),
        region     = os.environ.get("AWS_REGION",     "eu-central-1"),
        project    = os.environ.get("GCP_PROJECT",    ""),
        azure_connection_string = os.environ.get("AZURE_MONITOR_CONNECTION_STRING", ""),
    )
    log = logging.getLogger("ekg_etl")

    # ------------------------------------------------------------------
    # 2. Secrets chain
    # ------------------------------------------------------------------
    secret_providers = [
        p.strip()
        for p in os.environ.get("EKG_SECRET_PROVIDERS", "aws,env").split(",")
    ]
    secrets = build_secrets_chain(
        secret_providers,
        region           = os.environ.get("AWS_REGION",      "eu-central-1"),
        vault_url        = os.environ.get("AZURE_VAULT_URL", ""),
        project          = os.environ.get("GCP_PROJECT",     ""),
        vault_addr       = os.environ.get("VAULT_ADDR",      "http://localhost:8200"),
        vault_token      = os.environ.get("VAULT_TOKEN"),
        role_id          = os.environ.get("VAULT_ROLE_ID"),
        secret_id        = os.environ.get("VAULT_SECRET_ID"),
        # INI file provider: explicit path via env var; falls back to
        # ./credentials.ini then ~/.ekg/credentials.ini when not set.
        # Recommended chain position: after cloud providers, before env vars.
        # Example: EKG_SECRET_PROVIDERS=aws,vault,ini,env
        credentials_file = os.environ.get("EKG_CREDENTIALS_FILE"),
    )

    # ------------------------------------------------------------------
    # 3. Tenant
    # ------------------------------------------------------------------
    tenant = TenantConfig(
        gsr_client = os.environ["GSR_CLIENT_ID"],
        gsr_inst   = os.environ["GSR_INST_ID"],
    )

    # ------------------------------------------------------------------
    # 4. Meta DB  (read-only)
    # ------------------------------------------------------------------
    meta_cfg = DBConfig(
        host       = os.environ.get("META_DB_HOST",   "localhost"),
        port       = int(os.environ.get("META_DB_PORT", "5432")),
        database   = os.environ.get("META_DB_NAME",   "meta"),
        schema     = os.environ.get("META_DB_SCHEMA", "meta"),
        secret_ref = os.environ.get("META_SECRET_REF", "prod/meta-db"),
    )

    # ------------------------------------------------------------------
    # 5. Run DB  (read-write)
    # ------------------------------------------------------------------
    run_cfg = DBConfig(
        host       = os.environ.get("RUN_DB_HOST",   "run-db-host"),
        port       = int(os.environ.get("RUN_DB_PORT", "5432")),
        database   = os.environ.get("RUN_DB_NAME",   "run"),
        schema     = os.environ.get("RUN_DB_SCHEMA", "run"),
        secret_ref = os.environ.get("RUN_SECRET_REF", "prod/run-db"),
    )

    # ------------------------------------------------------------------
    # 6. Graph target
    #    EKG_GRAPH_TARGET selects the implementation.
    #    All target options are collected from EKG_TARGET_* env vars.
    # ------------------------------------------------------------------
    graph_target_name = os.environ.get("EKG_GRAPH_TARGET", "neptune").lower()

    # Resolve graph target credentials via the secrets chain
    graph_secret_ref = os.environ.get("EKG_TARGET_SECRET_REF", "prod/ekg-target")
    graph_creds: dict = {}
    try:
        graph_creds = secrets.resolve(graph_secret_ref)
    except Exception:
        log.debug("Graph target credentials not found via secrets chain; using env vars.")

    target_options: Dict[str, Any] = {
        # Common — overridden by graph_creds when present
        "endpoint":      graph_creds.get("endpoint", os.environ.get("EKG_TARGET_ENDPOINT",  "")),
        "username":      graph_creds.get("username", os.environ.get("EKG_TARGET_USERNAME", "")),
        "password":      graph_creds.get("password", os.environ.get("EKG_TARGET_PASSWORD", "")),
        "database":      graph_creds.get("database", os.environ.get("EKG_TARGET_DATABASE",  "")),
        "region":        os.environ.get("AWS_REGION",           "eu-central-1"),
        # Neptune-specific
        "s3_staging":    os.environ.get("EKG_TARGET_S3_STAGING",   ""),
        "iam_role_arn":  os.environ.get("EKG_TARGET_IAM_ROLE",     ""),
        "concurrency":   int(os.environ.get("EKG_TARGET_CONCURRENCY", "2")),
        "fail_on_error": os.environ.get("EKG_TARGET_FAIL_ON_ERROR", "false").lower() == "true",
        # Neo4j-specific
        "bulk_batch":    int(os.environ.get("EKG_TARGET_BULK_BATCH", "500")),
        # Cosmos-specific
        "graph":               os.environ.get("EKG_TARGET_GRAPH", ""),
        "partition_key":       graph_creds.get("partition_key", os.environ.get("EKG_TARGET_PARTITION_KEY", "partitionKey")),
        "partition_key_value": f"{tenant.gsr_client}:{tenant.gsr_inst}",
        # Spanner-specific
        "project":       os.environ.get("GCP_PROJECT",              ""),
        "instance":      os.environ.get("EKG_TARGET_INSTANCE",      ""),
        "node_table":    os.environ.get("EKG_TARGET_NODE_TABLE",    "EKGNode"),
        "edge_table":    os.environ.get("EKG_TARGET_EDGE_TABLE",    "EKGEdge"),
    }

    graph_target_cfg = GraphTargetConfig(target=graph_target_name, options=target_options)
    graph_target     = create_graph_target(graph_target_name, graph_target_cfg)

    # ------------------------------------------------------------------
    # 7. Run
    # ------------------------------------------------------------------
    pipeline = EKGETLPipeline(
        meta_cfg     = meta_cfg,
        run_cfg      = run_cfg,
        target       = graph_target,
        tenant       = tenant,
        secrets      = secrets,
        bulk_staging = target_options.get("s3_staging", ""),
    )

    triggered_by = os.environ.get("USER", "local")
    if args.mode == "full":
        pipeline.run_full(triggered_by=triggered_by)
    else:
        pipeline.run_incremental(triggered_by=triggered_by)

