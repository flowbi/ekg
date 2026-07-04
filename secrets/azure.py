"""
secrets/azure.py  –  Azure Key Vault provider.

pip install azure-keyvault-secrets azure-identity

ref     Secret name within the configured vault.
        For JSON-structured secrets (multiple keys in one secret), the
        secret value must be a JSON string.
        For simple username/password, use two secrets and name them
        "<ref>-username" and "<ref>-password"; the provider merges them.
"""

from __future__ import annotations

import json
import logging
from typing import Dict, Optional

from secrets.base import SecretsProvider

log = logging.getLogger("ekg_etl.secrets.azure")


class AzureKeyVaultProvider(SecretsProvider):
    """
    Resolves credentials from Azure Key Vault.

    Authentication uses DefaultAzureCredential (env vars, managed identity,
    Azure CLI, etc.) — no credentials are stored in this class.

    Two resolution strategies (tried in order):
    1. The secret named *ref* exists and its value is a JSON dict.
    2. Secrets named "<ref>-username" and "<ref>-password" exist.
    """

    def __init__(self, vault_url: str) -> None:
        if not vault_url:
            raise ValueError("AzureKeyVaultProvider requires vault_url")
        self._vault_url = vault_url.rstrip("/")
        self._client    = None

    def _kv(self):
        if self._client is None:
            from azure.keyvault.secrets import SecretClient       # type: ignore
            from azure.identity         import DefaultAzureCredential  # type: ignore
            self._client = SecretClient(
                vault_url=self._vault_url,
                credential=DefaultAzureCredential(),
            )
        return self._client

    def _get(self, name: str) -> Optional[str]:
        from azure.core.exceptions import ResourceNotFoundError  # type: ignore
        try:
            return self._kv().get_secret(name).value
        except ResourceNotFoundError:
            return None

    def resolve(self, ref: str) -> Optional[Dict[str, str]]:
        # Strategy 1: single JSON secret
        raw = self._get(ref)
        if raw is not None:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    log.debug("Azure KV: resolved '%s' as JSON secret", ref)
                    return parsed
            except json.JSONDecodeError:
                pass

        # Strategy 2: split username / password secrets
        username = self._get(f"{ref}-username")
        password = self._get(f"{ref}-password")
        if username is not None and password is not None:
            log.debug("Azure KV: resolved '%s' from split secrets", ref)
            return {"username": username, "password": password}

        log.debug("Azure KV: secret '%s' not found in vault %s", ref, self._vault_url)
        return None
