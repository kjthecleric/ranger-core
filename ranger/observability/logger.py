"""Structured JSON logger for Ranger — all pipeline events are logged as JSON.

Configures structlog to emit JSON-formatted logs with pipeline context.
"""

from __future__ import annotations

import logging
import sys

import structlog


def configure_logging(log_level: str = "INFO") -> None:
    """Configure structlog for JSON-formatted, structured logging.

    Args:
        log_level: Minimum log level (DEBUG, INFO, WARNING, ERROR).
    """
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, log_level, logging.INFO)),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )


def get_pipeline_logger(pipeline_name: str) -> structlog.BoundLogger:
    """Get a logger bound with pipeline context.

    Args:
        pipeline_name: Name of the pipeline for log context.

    Returns:
        A structlog BoundLogger with pipeline context.
    """
    return structlog.get_logger().bind(pipeline=pipeline_name, component="ranger")
