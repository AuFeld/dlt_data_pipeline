"""Secret-scrubbing log filter (Segment 12).

dlt and Airflow both echo connection strings + tokens into logs/tracebacks on
failure. This module installs a ``logging.Filter`` on the root logger plus
``dlt`` / ``airflow`` / ``airflow.task`` named loggers that rewrites obvious
credential patterns to ``***`` before they hit any handler.

Patterns covered:
  * ``password=…`` / ``token=…`` / ``secret=…`` / ``api_key=…`` style kwargs.
  * URI ``userinfo`` components (``scheme://user:pass@host``).
  * AWS ``aws_access_key_id`` / ``aws_secret_access_key`` style kwargs.

Install sites:
  * ``dags/data_pipeline_dags.py`` — DagBag-parse time, covers scheduler +
    worker pod processes.
  * ``src/dlt_data_pipeline/__main__.py:main`` — CLI entry, covers ``python -m
    dlt_data_pipeline …`` invocations (including KubernetesExecutor per-task
    pods that exec the CLI).

``install_secret_scrub`` is idempotent — repeated calls add the filter once
per logger via an ``in`` check on ``logger.filters``.
"""

from __future__ import annotations

import logging
import re

_KV_PATTERN = re.compile(
    r"(?P<key>password|token|secret|api_key|access_key|secret_key|"
    r"aws_access_key_id|aws_secret_access_key)"
    r"\s*=\s*"
    r"(?P<val>[^\s&'\";)]+)",
    re.IGNORECASE,
)

# Scheme intentionally limited to the connector schemes dlt actually emits, so
# we don't accidentally scrub innocuous URLs like https://docs.example.com/
# that have no userinfo segment.
_URI_USERINFO_PATTERN = re.compile(
    r"(?P<scheme>postgres(?:ql)?|mysql\+?\w*|snowflake|databricks|"
    r"redshift|mssql\+?\w*)://"
    r"(?P<user>[^:/\s]+):(?P<pass>[^@\s]+)@",
    re.IGNORECASE,
)

_MASK = "***"

_TARGET_LOGGERS: tuple[str, ...] = (
    # root logger ("" resolves to logging.root)
    "",
    "dlt",
    "airflow",
    "airflow.task",
)


def _scrub(message: str) -> str:
    """Apply both patterns to a single rendered string."""
    message = _KV_PATTERN.sub(lambda m: f"{m.group('key')}={_MASK}", message)
    message = _URI_USERINFO_PATTERN.sub(
        lambda m: f"{m.group('scheme')}://{m.group('user')}:{_MASK}@", message
    )
    return message


class SecretScrubFilter(logging.Filter):
    """Rewrite LogRecord.msg + args + exception text to mask credentials.

    The filter mutates the record in place and returns ``True`` (never drops
    records). It scrubs the fully-rendered message — args get folded into
    ``msg`` and cleared so downstream handlers don't re-substitute.
    """

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        try:
            rendered = record.getMessage()
        except Exception:
            # If formatting itself blew up, fall back to the raw template so
            # we never lose a log line to the filter.
            rendered = str(record.msg)
        scrubbed = _scrub(rendered)
        if scrubbed != rendered:
            record.msg = scrubbed
            record.args = None
        if record.exc_info and record.exc_info[1] is not None:
            # Mutate the exception's args so the traceback formatter picks up
            # the scrubbed text. Best effort — some exceptions are frozen.
            exc = record.exc_info[1]
            try:
                exc.args = tuple(_scrub(str(a)) if isinstance(a, str) else a for a in exc.args)
            except (AttributeError, TypeError):
                pass
        return True


def install_secret_scrub() -> None:
    """Attach a shared ``SecretScrubFilter`` to root + dlt + airflow loggers.

    Idempotent: repeat calls are no-ops because we check ``in logger.filters``
    before adding. Safe to call multiple times in the same process (e.g.
    DagBag re-parse + CLI invocation in the same Airflow task pod).
    """
    shared = SecretScrubFilter(name="dlt_data_pipeline.secret_scrub")
    for name in _TARGET_LOGGERS:
        logger = logging.getLogger(name) if name else logging.root
        if not any(isinstance(f, SecretScrubFilter) for f in logger.filters):
            logger.addFilter(shared)
