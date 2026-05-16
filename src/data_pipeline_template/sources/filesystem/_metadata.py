from __future__ import annotations

from data_pipeline_template.sources._metadata import SourceTypeMetadata

metadata = SourceTypeMetadata(
    description="Filesystem source (local + S3, CSV/Parquet/JSONL). Builder lands in Segment 8.",
    env_var_template="SOURCES__FILESYSTEM__<CONNECTION>__CREDENTIALS",
    allowed_config_keys=(),
    required_config_keys=(),
    notes="Stub. Allowed config keys + required fields will be populated when the builder lands.",
)
