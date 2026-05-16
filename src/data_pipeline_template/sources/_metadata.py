"""Source-type metadata for introspection.

Each source package exposes a module-level ``metadata: SourceTypeMetadata``
constant, registered via the ``data_pipeline_template.sources.metadata``
entry-point group (sibling to the builder group). The CLI ``sources describe``
and ``pipelines doctor`` subcommands consume this without importing source
implementations directly, so out-of-tree source packages can declare their own
metadata the same way.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceTypeMetadata:
    """Describes a source type for ``sources describe`` / ``pipelines doctor``.

    ``env_var_template`` uses the literal token ``<CONNECTION>`` as a
    placeholder; the CLI substitutes it with the YAML's
    ``source.connection`` value (uppercased) when reporting expected vars.
    Sources that need no credentials (e.g. public REST APIs) set this to
    ``None``.
    """

    description: str
    env_var_template: str | None
    allowed_config_keys: tuple[str, ...] = ()
    required_config_keys: tuple[str, ...] = ()
    notes: str = ""

    def resolve_env_var(self, connection: str) -> str | None:
        if self.env_var_template is None:
            return None
        return self.env_var_template.replace("<CONNECTION>", connection.upper())
