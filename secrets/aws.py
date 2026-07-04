"""
secrets/aws.py  –  AWS Secrets Manager provider.

pip install boto3
"""

from __future__ import annotations

import json
import logging
from typing import Dict, Optional

from secrets.base import SecretsProvider

log = logging.getLogger("ekg_etl.secrets.aws")


class AwsSecretsProvider(SecretsProvider):
    """
    Resolves credentials from AWS Secrets Manager.

    ref     Secret name or full ARN.
    Returns JSON-parsed secret dict, or None if secret not found.
    """

    def __init__(self, region: str = "eu-central-1") -> None:
        self._region = region
        self._client = None

    def _sm(self):
        if self._client is None:
            import boto3
            self._client = boto3.client("secretsmanager", region_name=self._region)
        return self._client

    def resolve(self, ref: str) -> Optional[Dict[str, str]]:
        from botocore.exceptions import ClientError
        try:
            resp = self._sm().get_secret_value(SecretId=ref)
            raw  = resp.get("SecretString") or resp.get("SecretBinary", b"").decode()
            log.debug("AWS: resolved '%s'", ref)
            return json.loads(raw)
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("ResourceNotFoundException", "SecretNotFoundException"):
                return None
            raise RuntimeError(f"AWS Secrets Manager error for '{ref}': {exc}") from exc
