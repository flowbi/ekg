"""
secrets/env.py  –  Environment variable provider (universal fallback).

ref     Env-var prefix string, e.g. "PROD_META_DB"
        Reads:  <PREFIX>_USERNAME  (required)
                <PREFIX>_PASSWORD  (required)
                <PREFIX>_HOST      (optional)
                <PREFIX>_PORT      (optional)
                <PREFIX>_DBNAME    (optional)
                <PREFIX>_ENDPOINT  (optional, for graph targets)
"""

from __future__ import annotations

import logging
import os
from typing import Dict, Optional

from secrets.base import SecretsProvider

log = logging.getLogger("ekg_etl.secrets.env")


class EnvSecretsProvider(SecretsProvider):
    """
    Reads credentials from environment variables using a prefix convention.

    Returns None (not raises) when the required USERNAME or PASSWORD
    variable is absent, allowing the chain to continue to the next provider.
    This is the intended behaviour when env vars are used as a fallback.
    """

    _OPTIONAL_KEYS = ["HOST", "PORT", "DBNAME", "ENDPOINT", "REGION"]

    def resolve(self, ref: str) -> Optional[Dict[str, str]]:
        prefix = ref.upper().rstrip("_") + "_"
        username = os.environ.get(prefix + "USERNAME")
        password = os.environ.get(prefix + "PASSWORD")

        if not username or not password:
            log.debug("Env: prefix '%s' — USERNAME or PASSWORD not set.", prefix)
            return None

        result: Dict[str, str] = {"username": username, "password": password}
        for key in self._OPTIONAL_KEYS:
            val = os.environ.get(prefix + key)
            if val:
                result[key.lower()] = val

        log.debug("Env: resolved credentials for prefix '%s'.", prefix)
        return result
