"""Filesystem source builder.

Wraps ``dlt.sources.filesystem`` for local + remote (S3 / GCS / Azure) reads
of CSV / Parquet / JSONL files. The YAML ``source.config`` block maps to:

- ``bucket_url`` (required): ``file://…`` for local paths, ``s3://…`` /
  ``gs://…`` / ``az://…`` for remote buckets.
- ``file_glob``: pattern relative to ``bucket_url`` (default: ``*``).
- ``files_per_page``: paging window for the listing (default: 100).
- ``extract_content``: read each file's content into the item (default: False).
- ``format``: one of ``csv | parquet | jsonl``. When omitted, infers from
  the ``file_glob`` extension; ``*`` patterns require an explicit ``format``.
- ``table_name``: dlt resource / destination table name (default: derived
  from ``source.connection``).
- ``reader_kwargs``: dict forwarded to the per-format reader (e.g.
  ``{"chunksize": 5000}`` for CSV, ``{"use_pyarrow": true}`` for Parquet).

Credentials are needed only for remote buckets. Resolution order:
``SOURCES__FILESYSTEM__<CONNECTION_UPPER>__CREDENTIALS`` env var → dlt's
native ``[sources.filesystem.<connection>.credentials]`` lookup. Local
``file://`` and absolute paths never trigger credential resolution.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse

import dlt
from dlt.extract import DltSource
from dlt.sources.filesystem import filesystem, read_csv, read_jsonl, read_parquet

from dlt_data_pipeline.config.models import SourceConfig, SourceFilesystem

_ALLOWED_KEYS: frozenset[str] = frozenset(
    {
        "bucket_url",
        "file_glob",
        "files_per_page",
        "extract_content",
        "format",
        "table_name",
        "reader_kwargs",
    }
)

_REMOTE_SCHEMES: frozenset[str] = frozenset({"s3", "gs", "gcs", "az", "abfs", "abfss"})

_READERS: dict[str, Callable[..., Any]] = {
    "csv": read_csv,
    "parquet": read_parquet,
    "jsonl": read_jsonl,
}


def _infer_format(file_glob: str) -> str | None:
    suffix = os.path.splitext(file_glob)[1].lstrip(".").lower()
    if suffix in _READERS:
        return suffix
    return None


def _is_remote(bucket_url: str) -> bool:
    scheme = urlparse(bucket_url).scheme.lower()
    return scheme in _REMOTE_SCHEMES


def _resolve_credentials(connection_name: str) -> Any:
    key = f"sources.filesystem.{connection_name}.credentials"
    try:
        value = dlt.secrets[key]
    except Exception:
        value = None
    if value is None:
        env_var = f"SOURCES__FILESYSTEM__{connection_name.upper()}__CREDENTIALS"
        raise ValueError(
            f"filesystem source {connection_name!r}: missing credentials for remote bucket. "
            f"Set env var {env_var} or add [{key}] to .dlt/secrets.toml"
        )
    return value


def builder(config: SourceConfig) -> DltSource:
    if not isinstance(config, SourceFilesystem):
        raise TypeError(f"filesystem builder received wrong config type: {type(config).__name__}")
    cfg = dict(config.config)
    unknown = set(cfg) - _ALLOWED_KEYS
    if unknown:
        raise ValueError(
            f"filesystem source {config.connection!r}: unknown config keys {sorted(unknown)}; "
            f"allowed: {sorted(_ALLOWED_KEYS)}"
        )
    bucket_url = cfg.get("bucket_url")
    if not bucket_url:
        raise ValueError(
            f"filesystem source {config.connection!r}: source.config.bucket_url is required"
        )
    file_glob = cfg.get("file_glob", "*")
    fmt = cfg.get("format") or _infer_format(file_glob)
    if fmt is None:
        raise ValueError(
            f"filesystem source {config.connection!r}: cannot infer format from file_glob "
            f"{file_glob!r}; set source.config.format to one of {sorted(_READERS)}"
        )
    if fmt not in _READERS:
        raise ValueError(
            f"filesystem source {config.connection!r}: unsupported format {fmt!r}; "
            f"supported: {sorted(_READERS)}"
        )

    reader_kwargs = cfg.get("reader_kwargs") or {}
    table_name = cfg.get("table_name", config.connection)

    listing_kwargs: dict[str, Any] = {
        "bucket_url": bucket_url,
        "file_glob": file_glob,
        "files_per_page": cfg.get("files_per_page", 100),
        "extract_content": cfg.get("extract_content", False),
    }
    if _is_remote(bucket_url):
        listing_kwargs["credentials"] = _resolve_credentials(config.connection)

    reader = _READERS[fmt]

    @dlt.source(name=config.connection)
    def _filesystem_source() -> Any:
        files = filesystem(**listing_kwargs)
        return (files | reader(**reader_kwargs)).with_name(table_name)

    return _filesystem_source()
