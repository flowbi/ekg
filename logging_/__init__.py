"""
logging_  –  EKG logging handler abstraction layer.

LoggingBuilder assembles Python logging handlers from a list of target names.
Console is always available as a fallback.

Supported targets
─────────────────
  console      StreamHandler to stdout (always added when no other succeeds)
  cloudwatch   AWS CloudWatch Logs        (pip install watchtower)
  azure        Azure Monitor / Log Analytics (pip install azure-monitor-opentelemetry-exporter)
  google       Google Cloud Logging       (pip install google-cloud-logging)
"""

from logging_.builder import LoggingBuilder, configure_logging

__all__ = ["LoggingBuilder", "configure_logging"]
