"""REST API source builder.

Maps the simplified YAML shape (``base_url`` + ``endpoints``) to the dlt
``rest_api_source`` config (``client.base_url`` + ``resources``). Extra dlt
keys passed through verbatim under ``client`` / ``resources`` / ``resource_defaults``
so power users keep full dlt fidelity without us re-modeling the schema.
"""

from __future__ import annotations

from typing import Any, cast

from dlt.extract import DltSource
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import RESTAPIConfig

from dlt_data_pipeline.config.models import SourceConfig, SourceRestApi


def _to_dlt_config(name: str, user_cfg: dict[str, Any]) -> dict[str, Any]:
    user_cfg = dict(user_cfg)
    client = dict(user_cfg.pop("client", {}))
    if "base_url" in user_cfg:
        client.setdefault("base_url", user_cfg.pop("base_url"))
    resources = user_cfg.pop("resources", None)
    if resources is None and "endpoints" in user_cfg:
        resources = user_cfg.pop("endpoints")
    if not client.get("base_url"):
        raise ValueError(
            f"rest_api source {name!r}: source.config.base_url (or client.base_url) is required"
        )
    if not resources:
        raise ValueError(
            f"rest_api source {name!r}: source.config.endpoints (or resources) is required"
        )
    dlt_cfg: dict[str, Any] = {"client": client, "resources": resources, **user_cfg}
    return dlt_cfg


def builder(config: SourceConfig) -> DltSource:
    if not isinstance(config, SourceRestApi):
        raise TypeError(f"rest_api builder received wrong config type: {type(config).__name__}")
    dlt_cfg = _to_dlt_config(config.connection, config.config)
    return rest_api_source(cast(RESTAPIConfig, dlt_cfg), name=config.connection)
