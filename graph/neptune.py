"""
graph/neptune.py  –  AWS Neptune graph target.

Incremental writes  →  Gremlin HTTP endpoint  (port 8182 /gremlin)
Bulk load           →  Neptune Bulk Loader REST API via S3 CSV staging

Authentication
──────────────
All requests to Neptune are signed with AWS SigV4 via requests-aws4auth.
This is required when IAM database authentication is enabled on the cluster
(which is the recommended and default secure configuration).

Credentials are resolved from the standard AWS credential chain:
  1. Environment variables (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
  2. ~/.aws/credentials profile
  3. EC2/ECS/Lambda instance role (recommended for production)

Required options (GraphTargetConfig.options)
────────────────────────────────────────────
  endpoint        str   Neptune cluster endpoint URL incl. port
                        e.g. "https://my-cluster.cluster-xxxx.eu-central-1.neptune.amazonaws.com:8182"
  s3_staging      str   S3 URI prefix for CSV staging, e.g. "s3://my-bucket/ekg-bulk"
  iam_role_arn    str   IAM role ARN Neptune uses to read from S3 (bulk load only)
  region          str   AWS region, e.g. "eu-central-1"
  concurrency     int   Neptune loader parallelism 1-8 (default 2)
  fail_on_error   bool  Neptune failOnError flag (default False)
  poll_interval   int   Seconds between loader status polls (default 10)
  poll_timeout    int   Max seconds to wait for loader completion (default 3600)
  iam_auth        bool  Sign requests with SigV4 (default True).
                        Set to False only when IAM auth is disabled on the cluster.

Dependencies
────────────
  pip install boto3 requests requests-aws4auth
"""

from __future__ import annotations

import csv
import io
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
import requests
from requests import Session

from graph.base import BulkRecord, GraphTarget, GraphTargetConfig

log = logging.getLogger("ekg_etl.graph.neptune")

_CONCURRENCY_MAP = {1: "LOW", 2: "MEDIUM", 4: "HIGH", 8: "OVERSUBSCRIBE"}


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


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


def _build_signed_session(region: str) -> Session:
    """
    Build a requests.Session whose every request is automatically signed
    with AWS SigV4 for the neptune-db service.

    Credentials are resolved from the standard boto3 credential chain
    (env vars → ~/.aws/credentials → instance role).  The session
    refreshes credentials automatically when temporary tokens expire.
    """
    try:
        from requests_aws4auth import AWS4Auth  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "requests-aws4auth is required for Neptune IAM authentication. "
            "Run: pip install requests-aws4auth"
        ) from exc

    boto_session = boto3.Session()
    credentials  = boto_session.get_credentials().get_frozen_credentials()

    auth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        "neptune-db",
        session_token=credentials.token,
    )

    session = Session()
    session.auth = auth
    return session


class NeptuneGraphTarget(GraphTarget):
    """
    AWS Neptune implementation of GraphTarget.

    Incremental path  →  Gremlin HTTP (coalesce/unfold upsert pattern)
                         with SigV4-signed requests (IAM auth).
    Bulk path         →  Neptune Bulk Loader REST API reading from S3 CSV files
                         (also SigV4-signed).
    """

    def __init__(self, config: GraphTargetConfig) -> None:
        super().__init__(config)
        self._endpoint      = config.get("endpoint", "").rstrip("/")
        self._gremlin_url   = f"{self._endpoint}/gremlin"
        self._loader_url    = f"{self._endpoint}/loader"
        self._s3_staging    = config.get("s3_staging", "").rstrip("/")
        self._iam_role_arn  = config.get("iam_role_arn", "")
        self._region        = config.get("region", "eu-central-1")
        self._concurrency   = config.get("concurrency", 2)
        self._fail_on_error = config.get("fail_on_error", False)
        self._poll_interval = config.get("poll_interval", 10)
        self._poll_timeout  = config.get("poll_timeout", 3600)
        self._iam_auth      = config.get("iam_auth", True)

        if not self._endpoint:
            raise ValueError(
                "Neptune endpoint is not configured. "
                "Set EKG_TARGET_ENDPOINT to the full cluster URL including port, e.g.: "
                "https://my-cluster.cluster-xxxx.eu-central-1.neptune.amazonaws.com:8182"
            )

        # Build the HTTP session — signed when IAM auth is enabled
        if self._iam_auth:
            self._session = _build_signed_session(self._region)
            log.info(
                "Neptune target: SigV4 signing enabled (region=%s endpoint=%s)",
                self._region, self._endpoint,
            )
        else:
            self._session = Session()
            log.warning(
                "Neptune target: SigV4 signing DISABLED. "
                "Only use this when IAM auth is off on the cluster."
            )

        # Bulk load state
        self._bulk_run_prefix: str        = ""
        self._node_buf: Optional[io.BytesIO] = None
        self._edge_buf: Optional[io.BytesIO] = None
        self._node_writer                 = None
        self._edge_writer                 = None
        self._s3 = boto3.client("s3", region_name=self._region)

    # ------------------------------------------------------------------
    # Internal HTTP helper
    # ------------------------------------------------------------------

    def _post(self, url: str, **kwargs) -> requests.Response:
        resp = self._session.post(url, timeout=60, **kwargs)
        resp.raise_for_status()
        return resp

    def _get(self, url: str, **kwargs) -> requests.Response:
        resp = self._session.get(url, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp

    def _gremlin(self, query: str) -> None:
        self._post(self._gremlin_url, json={"gremlin": query})

    # ------------------------------------------------------------------
    # Precheck
    # ------------------------------------------------------------------

    def _do_precheck(self) -> None:
        """
        GET /status — lightest authenticated request Neptune supports.
        Confirms network reachability, TLS, and SigV4 credentials in one call.
        """
        status_url = f"{self._endpoint}/status"
        log.debug("Neptune precheck: GET %s", status_url)
        resp = self._get(status_url)
        body = resp.json()
        db_status = body.get("status", "unknown")
        log.info("Neptune status: %s  engine: %s",
                 db_status, body.get("dbEngineVersion", "?"))
        if db_status != "healthy":
            from graph.base import PreCheckError
            raise PreCheckError(
                f"Neptune cluster status is '{db_status}', expected 'healthy'. "
                f"Full response: {body}"
            )

    # ------------------------------------------------------------------
    # Incremental operations
    # ------------------------------------------------------------------

    def upsert_node(self, node_id: str, label: str, props: Dict[str, Any]) -> None:
        self._gremlin(
            f"g.V('{_esc(node_id)}').fold().coalesce("
            f"__.unfold(),"
            f"__.addV('{_esc(label)}').property(id,'{_esc(node_id)}')"
            f"){_prop_chain(props)}"
        )

    def upsert_edge(
        self, edge_id: str, label: str, from_id: str, to_id: str, props: Dict[str, Any]
    ) -> None:
        self._gremlin(
            f"g.E('{_esc(edge_id)}').fold().coalesce("
            f"__.unfold(),"
            f"__.addE('{_esc(label)}')"
            f".from_(g.V('{_esc(from_id)}'))"
            f".to(g.V('{_esc(to_id)}'))"
            f".property(id,'{_esc(edge_id)}')"
            f"){_prop_chain(props)}"
        )

    def delete_vertex(self, node_id: str) -> None:
        self._gremlin(f"g.V('{_esc(node_id)}').drop()")

    def delete_edge(self, edge_id: str) -> None:
        self._gremlin(f"g.E('{_esc(edge_id)}').drop()")

    def upsert_concept_tag(
        self, tag_id: str, tag_name: str, tag_category: str, display_name: Optional[str]
    ) -> None:
        dn = display_name or tag_name
        self._gremlin(
            f"g.V('{_esc(tag_id)}').fold().coalesce("
            f"__.unfold(),"
            f"__.addV('ConceptTag').property(id,'{_esc(tag_id)}')"
            f").property('name','{_esc(tag_name)}')"
            f".property('category','{_esc(tag_category)}')"
            f".property('display_name','{_esc(dn)}')"
        )

    def upsert_tagged_as(self, node_id: str, tag_id: str) -> None:
        edge_id = f"{node_id}__TAG__{tag_id}"
        self._gremlin(
            f"g.E('{_esc(edge_id)}').fold().coalesce("
            f"__.unfold(),"
            f"__.addE('TAGGED_AS')"
            f".from_(g.V('{_esc(node_id)}'))"
            f".to(g.V('{_esc(tag_id)}'))"
            f".property(id,'{_esc(edge_id)}')"
            f")"
        )

    # ------------------------------------------------------------------
    # Bulk load
    # ------------------------------------------------------------------

    def begin_bulk(self) -> None:
        run_id = f"run-{_utcnow()}-{uuid.uuid4().hex[:8]}"
        self._bulk_run_prefix = f"{self._s3_staging}/{run_id}"
        self._node_buf  = io.BytesIO()
        self._edge_buf  = io.BytesIO()
        node_wrap = io.TextIOWrapper(
            self._node_buf, encoding="utf-8", newline="", write_through=True
        )
        edge_wrap = io.TextIOWrapper(
            self._edge_buf, encoding="utf-8", newline="", write_through=True
        )
        self._node_writer = csv.writer(node_wrap)
        self._edge_writer = csv.writer(edge_wrap)
        log.debug("Neptune bulk: staging prefix %s", self._bulk_run_prefix)

    def write_bulk_node(self, record: BulkRecord) -> None:
        if self._node_writer is None:
            raise RuntimeError("begin_bulk() must be called before write_bulk_node()")
        row = [record.entity_id, record.label]
        for k, v in record.props.items():
            row.append(f"{k}={v}" if v is not None else "")
        self._node_writer.writerow(row)

    def write_bulk_edge(self, record: BulkRecord) -> None:
        if self._edge_writer is None:
            raise RuntimeError("begin_bulk() must be called before write_bulk_edge()")
        row = [record.entity_id, record.from_id, record.to_id, record.label]
        for k, v in record.props.items():
            row.append(f"{k}={v}" if v is not None else "")
        self._edge_writer.writerow(row)

    def commit_bulk(self) -> str:
        """Upload CSVs to S3 and trigger Neptune Bulk Loader. Returns loader job ID."""
        bucket, prefix = self._parse_s3_uri(self._bulk_run_prefix)

        for key_suffix, buf in [
            ("nodes.csv", self._node_buf),
            ("edges.csv", self._edge_buf),
        ]:
            buf.seek(0)
            self._s3.upload_fileobj(buf, bucket, f"{prefix}/{key_suffix}")
            log.info("Neptune bulk: uploaded s3://%s/%s/%s", bucket, prefix, key_suffix)

        source_uri = f"s3://{bucket}/{prefix}/"
        payload = {
            "source":         source_uri,
            "format":         "csv",
            "iamRoleArn":     self._iam_role_arn,
            "region":         self._region,
            "failOnError":    str(self._fail_on_error).lower(),
            "parallelism":    _CONCURRENCY_MAP.get(self._concurrency, "MEDIUM"),
            "updateSingleCardinalityProperties": "TRUE",
            "queueRequest":   "TRUE",
        }
        resp = self._post(self._loader_url, json=payload)
        job_id = resp.json()["payload"]["loadId"]
        log.info("Neptune bulk loader job ID: %s", job_id)

        # Poll to completion
        poll_url = f"{self._loader_url}/{job_id}"
        deadline = time.monotonic() + self._poll_timeout
        while time.monotonic() < deadline:
            r = self._get(poll_url)
            status = (
                r.json()
                 .get("payload", {})
                 .get("overallStatus", {})
                 .get("status", "UNKNOWN")
            )
            log.info("Neptune loader status: %s", status)
            if status in ("LOAD_COMPLETED", "LOAD_FAILED", "LOAD_CANCELLED"):
                if status != "LOAD_COMPLETED":
                    log.error("Neptune bulk load did not complete: %s", status)
                break
            time.sleep(self._poll_interval)
        else:
            raise TimeoutError(
                f"Neptune bulk load {job_id} timed out after {self._poll_timeout}s"
            )

        return job_id

    def close(self) -> None:
        self._session.close()
        self._node_buf = None
        self._edge_buf = None

    @staticmethod
    def _parse_s3_uri(uri: str):
        uri = uri.replace("s3://", "")
        bucket, _, prefix = uri.partition("/")
        return bucket, prefix
