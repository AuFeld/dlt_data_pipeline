"""Source builder Protocol.

Every source-type subpackage exposes a ``builder`` callable that takes a
validated source config (a discriminated union member from ``config.models``)
and returns a runnable ``dlt`` source. The registry (``sources.registry``)
discovers builders via the ``data_pipeline_template.sources`` entry-point group.
"""

from __future__ import annotations

from typing import Protocol

from dlt.extract import DltSource

from data_pipeline_template.config.models import SourceConfig


class Builder(Protocol):
    def __call__(self, config: SourceConfig) -> DltSource: ...
