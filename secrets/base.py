"""
secrets/base.py  –  SecretsProvider abstract base class and chaining logic.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

log = logging.getLogger("ekg_etl.secrets")


class SecretsProvider(ABC):
    """
    Abstract base class for credential resolution providers.

    Implementations must be stateless apart from optional caching.
    """

    @abstractmethod
    def resolve(self, ref: str) -> Optional[Dict[str, str]]:
        """
        Attempt to resolve credentials for *ref*.

        Returns a dict on success, or None if this provider cannot
        handle the reference (allowing the chain to continue).

        Should raise RuntimeError only for unrecoverable errors
        (e.g. auth failure, malformed secret), not for missing secrets
        (return None instead).
        """

    @property
    def provider_name(self) -> str:
        return self.__class__.__name__


class ChainedSecretsProvider:
    """
    Tries each SecretsProvider in order, returning the first non-None result.

    Usage
    ─────
    chain = ChainedSecretsProvider([AwsSecretsProvider(), EnvSecretsProvider()])
    creds = chain.resolve("prod/meta-db")
    # → {"username": "…", "password": "…", …}

    Results are cached per ref for the lifetime of the chain instance.
    """

    def __init__(self, providers: List[SecretsProvider]) -> None:
        if not providers:
            raise ValueError("ChainedSecretsProvider requires at least one provider")
        self._providers = providers
        self._cache: Dict[str, Dict[str, str]] = {}

    def resolve(self, ref: str) -> Dict[str, str]:
        if ref in self._cache:
            return self._cache[ref]

        for provider in self._providers:
            try:
                result = provider.resolve(ref)
            except Exception as exc:
                log.warning(
                    "%s failed for ref '%s': %s — trying next provider.",
                    provider.provider_name, ref, exc,
                )
                result = None

            if result is not None:
                log.debug(
                    "Resolved credentials for '%s' via %s.",
                    ref, provider.provider_name,
                )
                self._cache[ref] = result
                return result

        raise RuntimeError(
            f"No secrets provider could resolve ref '{ref}'. "
            f"Tried: {[p.provider_name for p in self._providers]}"
        )


def build_secrets_chain(provider_names: List[str], **kwargs) -> ChainedSecretsProvider:
    """
    Factory that builds a ChainedSecretsProvider from a list of provider names.

    Supported names (case-insensitive)
    ───────────────────────────────────
    'aws'    →  AwsSecretsProvider      (kwargs: region)
    'azure'  →  AzureKeyVaultProvider   (kwargs: vault_url)
    'google' →  GoogleSecretProvider    (kwargs: project)
    'vault'  →  HashiCorpVaultProvider  (kwargs: vault_addr, vault_token or role_id+secret_id)
    'ini'    →  IniSecretsProvider      (kwargs: credentials_file – path string or None)
    'env'    →  EnvSecretsProvider      (no kwargs)

    Insertion order
    ───────────────
    'env' is always appended at the end if not already in the list.
    Recommended order for mixed environments:
        cloud providers → 'ini' → 'env'
    e.g. EKG_SECRET_PROVIDERS=aws,vault,ini,env

    The 'ini' provider reads a plain-text INI file whose sections are
    named after secret refs.  File path resolution order:
      1. credentials_file kwarg (explicit path)
      2. EKG_CREDENTIALS_FILE environment variable
      3. ./credentials.ini
      4. ~/.ekg/credentials.ini

    Example
    ───────
    chain = build_secrets_chain(
        ['aws', 'ini', 'env'],
        region='eu-central-1',
        credentials_file='/etc/ekg/credentials.ini',
    )
    """
    from pathlib import Path as _Path

    from secrets.aws      import AwsSecretsProvider
    from secrets.azure    import AzureKeyVaultProvider
    from secrets.google   import GoogleSecretProvider
    from secrets.vault    import HashiCorpVaultProvider
    from secrets.ini_file import IniSecretsProvider
    from secrets.env      import EnvSecretsProvider

    # Resolve optional explicit credentials file path once
    _cred_file_raw = kwargs.get("credentials_file")
    _cred_file     = _Path(_cred_file_raw) if _cred_file_raw else None

    _MAP = {
        "aws":    lambda: AwsSecretsProvider(region=kwargs.get("region", "eu-central-1")),
        "azure":  lambda: AzureKeyVaultProvider(vault_url=kwargs.get("vault_url", "")),
        "google": lambda: GoogleSecretProvider(project=kwargs.get("project", "")),
        "vault":  lambda: HashiCorpVaultProvider(
            vault_addr=kwargs.get("vault_addr", "http://localhost:8200"),
            token=kwargs.get("vault_token"),
            role_id=kwargs.get("role_id"),
            secret_id=kwargs.get("secret_id"),
        ),
        "ini":    lambda: IniSecretsProvider(path=_cred_file),
        "env":    lambda: EnvSecretsProvider(),
    }

    names_lower = [n.lower() for n in provider_names]
    if "env" not in names_lower:
        names_lower.append("env")   # always last

    providers: List[SecretsProvider] = []
    for name in names_lower:
        if name not in _MAP:
            raise ValueError(
                f"Unknown secrets provider '{name}'. "
                f"Supported: {sorted(_MAP)}"
            )
        providers.append(_MAP[name]())

    return ChainedSecretsProvider(providers)
