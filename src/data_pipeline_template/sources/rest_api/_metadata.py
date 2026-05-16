from __future__ import annotations

from data_pipeline_template.sources._metadata import SourceTypeMetadata

metadata = SourceTypeMetadata(
    description=(
        "REST API source. Wraps dlt.sources.rest_api; maps YAML "
        "source.config.base_url + endpoints onto dlt's client + resources."
    ),
    env_var_template=None,
    allowed_config_keys=(
        "base_url",
        "endpoints",
        "client",
        "resources",
        "resource_defaults",
    ),
    required_config_keys=("base_url", "endpoints"),
    notes=(
        "No built-in credential resolution. If the API needs auth, declare "
        "it under source.config.client.auth per dlt's REST API source docs."
    ),
)
