"""`SecretScrubFilter` masks credentials in log records (Segment 12)."""

from __future__ import annotations

import logging

from dlt_data_pipeline.observability.log_filter import (
    SecretScrubFilter,
    install_secret_scrub,
)


def _apply(message: str, *args: object) -> str:
    """Run a single message through the filter and return the scrubbed text."""
    record = logging.LogRecord(
        name="test",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=args or None,
        exc_info=None,
    )
    SecretScrubFilter().filter(record)
    return record.getMessage()


def test_masks_password_kwarg() -> None:
    out = _apply("connect failed password=hunter2 user=foo")
    assert "hunter2" not in out
    assert "password=***" in out
    assert "user=foo" in out


def test_masks_token_kwarg_case_insensitive() -> None:
    out = _apply("auth Token=abc.def.ghi ok")
    assert "abc.def.ghi" not in out
    assert "Token=***" in out or "token=***" in out.lower()


def test_masks_postgres_uri_userinfo() -> None:
    out = _apply("retrying: postgresql://app:hunter2@db.example.com:5432/orders")
    assert "hunter2" not in out
    assert "app:***@db.example.com" in out


def test_preserves_innocuous_https_url() -> None:
    """https isn't in the scheme allowlist for userinfo, and bare `key=` isn't
    in the kv list — generic query strings should pass through untouched."""
    out = _apply("see https://docs.example.com/path?cache_key=value")
    assert out == "see https://docs.example.com/path?cache_key=value"


def test_masks_api_key_kwarg() -> None:
    out = _apply("call: api_key=sk-abcdef123")
    assert "sk-abcdef123" not in out
    assert "api_key=***" in out


def test_scrubs_exception_args() -> None:
    record = logging.LogRecord(
        name="t",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="boom",
        args=None,
        exc_info=(ValueError, ValueError("connect postgresql://u:s3cret@h/db"), None),
    )
    SecretScrubFilter().filter(record)
    assert "s3cret" not in str(record.exc_info[1])


def test_install_is_idempotent() -> None:
    install_secret_scrub()
    install_secret_scrub()
    root = logging.root
    scrubbers = [f for f in root.filters if isinstance(f, SecretScrubFilter)]
    assert len(scrubbers) == 1


def test_install_scrubs_attached_logger() -> None:
    """End-to-end through stdlib handlers: attach a capturing handler to the
    `dlt` logger after install, emit a credential-bearing record, assert the
    rendered output is scrubbed."""
    install_secret_scrub()
    log = logging.getLogger("dlt")
    captured: list[str] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(self.format(record))

    handler = _Capture(level=logging.ERROR)
    handler.setFormatter(logging.Formatter("%(message)s"))
    log.addHandler(handler)
    try:
        log.error("creds: postgresql://app:hunter2@db/x password=hunter2")
    finally:
        log.removeHandler(handler)
    text = "\n".join(captured)
    assert "hunter2" not in text
    assert "***" in text
