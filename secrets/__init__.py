"""
secrets  –  EKG credential resolution abstraction layer.

SecretsProvider ABC
───────────────────
Every provider implements resolve(ref) -> dict | None.
  ref     An opaque reference string whose meaning is provider-specific:
            AWS   →  secret name or ARN
            Azure →  Key Vault secret name (vault URL in config)
            GCP   →  "projects/P/secrets/S/versions/V" or short name
            Vault →  "secret/data/path"
            Env   →  env-var prefix, e.g. "PROD_CRM"

  Returns a dict with at minimum:
    { "username": "…", "password": "…" }
  Optionally overriding:
    host, port, dbname, endpoint, …

ChainedSecretsProvider
──────────────────────
Tries each provider in order, returning the first non-None result.
Env vars are always last in the chain so they act as a universal fallback.

Factory: build_secrets_chain(providers: list[str]) -> ChainedSecretsProvider
"""

from secrets.base     import SecretsProvider, ChainedSecretsProvider, build_secrets_chain
from secrets.ini_file import IniSecretsProvider

__all__ = ["SecretsProvider", "ChainedSecretsProvider", "build_secrets_chain", "IniSecretsProvider"]
