"""
logging_/azure_monitor.py  –  Azure Monitor / Application Insights handler.

pip install opencensus-ext-azure

Authentication: supply connection_string (preferred) or instrumentation_key.
Connection string format: "InstrumentationKey=...;IngestionEndpoint=..."
Obtain from: Azure Portal → Application Insights resource → Overview.

kwargs
──────
  azure_connection_string  str   Full App Insights connection string (preferred)
  azure_instrumentation_key str  Instrumentation key (fallback)
  log_level                int   Handler log level (default logging.WARNING
                                  because Azure Monitor charges per ingested event)
"""

from __future__ import annotations

import logging
from typing import Optional

_FMT = logging.Formatter(
    "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)


def build_azure_monitor_handler(
    azure_connection_string:   str = "",
    azure_instrumentation_key: str = "",
    log_level: int = logging.WARNING,
    **_,
) -> Optional[logging.Handler]:
    """
    Build and return an Azure Monitor (Application Insights) logging handler.

    Uses opencensus-ext-azure AzureLogHandler.  Records are sent as traces to
    Application Insights and are queryable via Log Analytics / KQL.
    """
    from opencensus.ext.azure.log_exporter import AzureLogHandler  # type: ignore

    if azure_connection_string:
        handler = AzureLogHandler(connection_string=azure_connection_string)
    elif azure_instrumentation_key:
        handler = AzureLogHandler(
            connection_string=f"InstrumentationKey={azure_instrumentation_key}"
        )
    else:
        raise ValueError(
            "Azure Monitor handler requires 'azure_connection_string' or "
            "'azure_instrumentation_key'."
        )

    handler.setFormatter(_FMT)
    handler.setLevel(log_level)
    logging.getLogger("ekg_etl").info("Azure Monitor logging handler installed.")
    return handler
