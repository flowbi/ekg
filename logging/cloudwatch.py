"""
logging_/cloudwatch.py  –  AWS CloudWatch Logs handler.

pip install watchtower

kwargs
──────
  log_group   str   CloudWatch log group name  (default "/ekg-etl")
  log_stream  str   CloudWatch log stream name (default "ekg-etl")
  region      str   AWS region                 (default "eu-central-1")
"""

from __future__ import annotations

import logging
from typing import Optional

_FMT = logging.Formatter(
    "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)


def build_cloudwatch_handler(
    log_group:  str = "/ekg-etl",
    log_stream: str = "ekg-etl",
    region:     str = "eu-central-1",
    **_,
) -> Optional[logging.Handler]:
    """Build and return a watchtower CloudWatch handler."""
    import boto3
    import watchtower  # type: ignore

    cw_client = boto3.client("logs", region_name=region)
    handler = watchtower.CloudWatchLogHandler(
        log_group=log_group,
        stream_name=log_stream,
        boto3_client=cw_client,
    )
    handler.setFormatter(_FMT)
    logging.getLogger("ekg_etl").info(
        "CloudWatch logging → %s / %s", log_group, log_stream
    )
    return handler
