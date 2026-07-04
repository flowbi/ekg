"""
secrets/google.py  –  Google Cloud Secret Manager provider.

pip install google-cloud-secret-manager

ref     Secret reference in one of two formats:
          Short:  "my-secret"          → latest version in configured project
          Full:   "projects/P/secrets/S/versions/V"
"""

from __future__ import annotations

import json
import logging
from typing import Dict, Optional

from secrets.base import SecretsProvider

log = logging.getLogger("ekg_etl.secrets.google")


class GoogleSecretProvider(SecretsProvider):
    """
    Resolves credentials from Google Cloud Secret Manager.

    Authentication uses Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS,
    service account attached to GCE/GKE, or gcloud CLI).
    """

    def __init__(self, project: str = "") -> None:
        self._project = project
        self._client  = None

    def _sm(self):
        if self._client is None:
            from google.cloud import secretmanager  # type: ignore
            self._client = secretmanager.SecretManagerServiceClient()
        return self._client

    def _full_name(self, ref: str) -> str:
        if ref.startswith("projects/"):
            return ref if "/versions/" in ref else f"{ref}/versions/latest"
        if not self._project:
            raise ValueError(
                "GoogleSecretProvider: short ref requires 'project' to be set at construction."
            )
        return f"projects/{self._project}/secrets/{ref}/versions/latest"

    def resolve(self, ref: str) -> Optional[Dict[str, str]]:
        from google.api_core.exceptions import NotFound  # type: ignore
        try:
            name     = self._full_name(ref)
            response = self._sm().access_secret_version(request={"name": name})
            raw      = response.payload.data.decode("utf-8")
            parsed   = json.loads(raw)
            log.debug("GCP Secret Manager: resolved '%s'", ref)
            return parsed if isinstance(parsed, dict) else None
        except NotFound:
            log.debug("GCP Secret Manager: secret '%s' not found", ref)
            return None
        except json.JSONDecodeError:
            log.warning("GCP Secret Manager: secret '%s' is not valid JSON — skipping.", ref)
            return None
