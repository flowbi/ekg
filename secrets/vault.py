"""
secrets/vault.py  –  HashiCorp Vault provider (KV v2).

pip install hvac

ref     Vault KV path, e.g. "secret/data/prod/meta-db"
        (the "secret/data/" prefix for KV v2 is added automatically
        if absent and mount_point is set).

Authentication: token (static) or AppRole (role_id + secret_id).
Token takes precedence when both are supplied.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

from secrets.base import SecretsProvider

log = logging.getLogger("ekg_etl.secrets.vault")


class HashiCorpVaultProvider(SecretsProvider):
    """
    Resolves credentials from HashiCorp Vault KV v2.

    Supports token auth and AppRole auth.  Token is preferred when set.
    """

    def __init__(
        self,
        vault_addr:  str            = "http://localhost:8200",
        token:       Optional[str]  = None,
        role_id:     Optional[str]  = None,
        secret_id:   Optional[str]  = None,
        mount_point: str            = "secret",
        namespace:   Optional[str]  = None,
    ) -> None:
        self._addr        = vault_addr
        self._token       = token
        self._role_id     = role_id
        self._secret_id   = secret_id
        self._mount_point = mount_point.rstrip("/")
        self._namespace   = namespace
        self._client      = None

    def _vault(self):
        if self._client is None:
            import hvac  # type: ignore
            client = hvac.Client(url=self._addr, namespace=self._namespace)
            if self._token:
                client.token = self._token
            elif self._role_id and self._secret_id:
                client.auth.approle.login(
                    role_id=self._role_id,
                    secret_id=self._secret_id,
                )
            else:
                raise RuntimeError(
                    "HashiCorpVaultProvider: supply either 'token' or 'role_id'+'secret_id'."
                )
            if not client.is_authenticated():
                raise RuntimeError("HashiCorpVaultProvider: Vault authentication failed.")
            self._client = client
        return self._client

    def _kv_path(self, ref: str) -> tuple[str, str]:
        """
        Split a ref like "secret/data/prod/meta-db" into (mount_point, path).
        Handles both "mount/data/path" (KV v2 canonical) and short "path" forms.
        """
        parts = ref.lstrip("/").split("/", 2)
        if len(parts) >= 3 and parts[1] == "data":
            # Full KV v2 path: <mount>/data/<path>
            return parts[0], parts[2]
        # Short path: use configured mount_point
        return self._mount_point, ref.lstrip("/")

    def resolve(self, ref: str) -> Optional[Dict[str, str]]:
        try:
            mount, path = self._kv_path(ref)
            secret = self._vault().secrets.kv.v2.read_secret_version(
                path=path, mount_point=mount
            )
            data = secret["data"]["data"]
            log.debug("Vault: resolved '%s' (mount=%s path=%s)", ref, mount, path)
            return data if isinstance(data, dict) else None
        except Exception as exc:
            # hvac raises hvac.exceptions.InvalidPath for missing secrets
            exc_name = type(exc).__name__
            if "InvalidPath" in exc_name or "NotFound" in exc_name:
                log.debug("Vault: path '%s' not found — %s", ref, exc)
                return None
            raise RuntimeError(f"Vault error for '{ref}': {exc}") from exc
