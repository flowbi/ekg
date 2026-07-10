"""
logging_/builder.py  –  LoggingBuilder and configure_logging helper.

Usage
─────
    from logging_ import configure_logging

    configure_logging(
        targets=["console", "cloudwatch", "google"],
        log_level=logging.INFO,
        log_group="/ekg-etl",
        log_stream="run-20240101T120000",
        region="eu-central-1",                   # CloudWatch
        project="my-gcp-project",               # Google
        workspace_id="...",                      # Azure
        workspace_key="...",                     # Azure
    )

Console is always added as a handler when:
  • "console" is in targets, OR
  • no other handler was successfully installed.
"""

from __future__ import annotations

import logging
import sys
from typing import List, Optional

log = logging.getLogger("ekg_etl")

_FMT = logging.Formatter(
    "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)


def _console_handler() -> logging.StreamHandler:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(_FMT)
    return h


class LoggingBuilder:
    """
    Assembles Python logging handlers for the 'ekg_etl' logger from a list
    of target names.  Each target is loaded lazily so that missing optional
    dependencies only raise an error when that specific target is requested.
    """

    def __init__(
        self,
        targets:    List[str],
        log_level:  int = logging.INFO,
        **kwargs,
    ) -> None:
        self._targets   = [t.lower() for t in targets]
        self._log_level = log_level
        self._kwargs    = kwargs

    def build(self) -> logging.Logger:
        logger = logging.getLogger("ekg_etl")
        logger.setLevel(self._log_level)
        logger.handlers.clear()
        logger.propagate = False

        want_console   = "console" in self._targets
        other_targets  = [t for t in self._targets if t != "console"]
        installed: List[str] = []

        for target in other_targets:
            handler = self._build_handler(target)
            if handler is not None:
                logger.addHandler(handler)
                installed.append(target)

        # Console: explicit request OR no other handler installed
        if want_console or not installed:
            logger.addHandler(_console_handler())
            if not want_console:
                logger.warning(
                    "No cloud logging handler was successfully installed; "
                    "falling back to console output."
                )

        return logger

    def _build_handler(self, target: str) -> Optional[logging.Handler]:
        try:
            if target == "cloudwatch":
                from logging_.cloudwatch    import build_cloudwatch_handler
                return build_cloudwatch_handler(**self._kwargs)
            if target == "azure":
                from logging_.azure_monitor import build_azure_monitor_handler
                return build_azure_monitor_handler(**self._kwargs)
            if target == "google":
                from logging_.google_logging import build_google_logging_handler
                return build_google_logging_handler(**self._kwargs)
            log.warning("Unknown logging target '%s'; skipping.", target)
            return None
        except ImportError as exc:
            log.warning("Logging target '%s' unavailable: %s", target, exc)
            return None
        except Exception as exc:
            log.warning("Failed to initialise logging target '%s': %s", target, exc)
            return None


def configure_logging(
    targets:   List[str],
    log_level: int = logging.INFO,
    **kwargs,
) -> logging.Logger:
    """
    Convenience wrapper.  Builds and installs handlers, returns the logger.

    Parameters
    ----------
    targets : list[str]
        Any combination of 'console', 'cloudwatch', 'azure', 'google'.
    log_level : int
        logging.INFO / logging.DEBUG / etc.
    **kwargs
        Passed through to each handler builder.
        See individual handler modules for supported keys.
    """
    return LoggingBuilder(targets=targets, log_level=log_level, **kwargs).build()
