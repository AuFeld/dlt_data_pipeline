from __future__ import annotations

from data_pipeline_template.sources._metadata import SourceTypeMetadata

metadata = SourceTypeMetadata(
    description="Filesystem source (local + S3/GCS/Azure; CSV/Parquet/JSONL).",
    env_var_template="SOURCES__FILESYSTEM__<CONNECTION>__CREDENTIALS",
    allowed_config_keys=(
        "bucket_url",
        "file_glob",
        "files_per_page",
        "extract_content",
        "format",
        "table_name",
        "reader_kwargs",
    ),
    required_config_keys=("bucket_url",),
    notes=(
        "Local paths use file:// or absolute paths and need no credentials. "
        "Remote buckets (s3://, gs://, az://) resolve credentials from the env "
        "var template above or [sources.filesystem.<connection>.credentials] "
        "in .dlt/secrets.toml. "
        "format: one of csv | parquet | jsonl (inferred from file_glob extension when omitted; "
        "required when file_glob is '*'). "
        "reader_kwargs forwards options to the per-format reader (e.g. {chunksize: 5000} "
        "for csv, {use_pyarrow: true} for parquet)."
    ),
)
