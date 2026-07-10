"""
logging_/google_logging.py  –  Google Cloud Logging handler.

pip install google-cloud-logging

Authentication uses Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS,
Workload Identity on GKE, or gcloud CLI login).

kwargs
──────
  project          str   GCP project ID (required)
  gcp_log_name     str   Cloud Logging log name (default "ekg-etl")
  resource         dict  Monitored resource descriptor (default: global)
                         e.g. {"type": "gce_instance", "labels": {"instance_id": "…"}}
"""

from __future__ import annotations

import logging
from typing import Optional

_FMT = logging.Formatter(
    "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)


def build_google_logging_handler(
    project:      str  = "",
    gcp_log_name: str  = "ekg-etl",
    resource:     dict = None,
    **_,
) -> Optional[logging.Handler]:
    """
    Build and return a Google Cloud Logging handler.

    The handler uses the CloudLoggingHandler from google-cloud-logging which
    streams structured log entries to Cloud Logging asynchronously.
    """
    import google.cloud.logging as gcp_logging  # type: ignore
    from google.cloud.logging.handlers import CloudLoggingHandler  # type: ignore

    if not project:
        raise ValueError("Google Cloud Logging handler requires 'project'.")

    client  = gcp_logging.Client(project=project)
    handler = CloudLoggingHandler(client, name=gcp_log_name, resource=resource)
    handler.setFormatter(_FMT)
    logging.getLogger("ekg_etl").info(
        "Google Cloud Logging handler installed → project=%s log=%s", project, gcp_log_name
    )
    return handler
