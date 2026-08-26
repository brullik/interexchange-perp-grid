from __future__ import annotations

import logging
import re
import sys
from typing import cast

import structlog
from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest

REGISTRY = CollectorRegistry(auto_describe=True)
SERVICE_UP = Gauge(
    "ipeg_service_up",
    "Whether the application service loop is running.",
    registry=REGISTRY,
)
SERVICE_STARTS = Counter(
    "ipeg_service_starts_total",
    "Number of service loop starts in this process.",
    registry=REGISTRY,
)
SERVICE_HEARTBEATS = Counter(
    "ipeg_service_heartbeats_total",
    "Number of persisted service heartbeats.",
    registry=REGISTRY,
)
LIVE_GUARD_DENIALS = Counter(
    "ipeg_live_guard_denials_total",
    "Live order guard denials by stable reason code.",
    labelnames=("reason",),
    registry=REGISTRY,
)

_TELEGRAM_TOKEN = re.compile(r"(?<![A-Za-z0-9_-])(?:bot)?\d{6,}:[A-Za-z0-9_-]{20,}")


class _SecretRedactionFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        redacted = _TELEGRAM_TOKEN.sub("<REDACTED_TELEGRAM_TOKEN>", message)
        if redacted != message:
            record.msg = redacted
            record.args = ()
        return True


def configure_logging(level: str) -> None:
    logging.basicConfig(format="%(message)s", level=level, stream=sys.stdout, force=True)
    for handler in logging.getLogger().handlers:
        handler.addFilter(_SecretRedactionFilter())
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    structlog.configure(
        processors=(
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.EventRenamer("event"),
            structlog.processors.JSONRenderer(sort_keys=True),
        ),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger() -> structlog.stdlib.BoundLogger:
    return cast(structlog.stdlib.BoundLogger, structlog.get_logger("interexchange_perp_grid"))


def render_metrics() -> str:
    return generate_latest(REGISTRY).decode("utf-8")
