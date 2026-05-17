"""MCP server — exposes Segment 6.5 introspection CLI as MCP tools.

Wraps the pure helpers in ``cli/sources_cmds.py`` and ``cli/pipelines_cmds.py``
so MCP clients (Claude Code, Codex, etc.) call them as native tools instead
of shelling out to ``python -m dlt_data_pipeline ...`` and parsing
stdout. Runs over stdio:

    uv run python -m dlt_data_pipeline.mcp_server

Registered for project-local discovery via ``.mcp.json`` at the repo root.
"""

from __future__ import annotations

from fastmcp import FastMCP

from dlt_data_pipeline.cli import pipelines_cmds, sources_cmds
from dlt_data_pipeline.sources import registry

mcp: FastMCP = FastMCP("dlt_data_pipeline")


@mcp.tool
def sources_list() -> list[str]:
    """List registered source types (entry-point discovered)."""
    return sources_cmds.list_sources()


@mcp.tool
def sources_describe(source_type: str) -> dict[str, object]:
    """Return env-var template, required/allowed config keys, and notes for one source type.

    Raises if ``source_type`` is unknown; the error message lists registered types.
    """
    try:
        return sources_cmds.describe_source(source_type)
    except (registry.UnknownSourceTypeError, registry.MissingSourceMetadataError) as exc:
        raise ValueError(str(exc)) from None


@mcp.tool
def pipelines_validate(
    pipelines_root: str = "pipelines",
    name: str | None = None,
    env: str | None = None,
) -> dict[str, object]:
    """Parse + validate one or all ``pipelines/*.yml``.

    Returns ``{status, pipelines_root, env, errors[], pipelines[]}``.
    ``status`` is ``"ok"`` or ``"error"``. When ``name`` is provided,
    validates only that pipeline. ``env`` selects which
    ``pipelines/_env/<env>.yml`` overlay applies; defaults to ``$DLT_ENV``
    then ``"dev"``.
    """
    return pipelines_cmds.validate_pipelines(pipelines_root, name, env=env)


@mcp.tool
def pipelines_doctor(
    pipelines_root: str = "pipelines",
    env: str | None = None,
) -> dict[str, object]:
    """Probe expected env vars + .dlt secrets for each pipeline. Never echoes secret values.

    Returns ``{status, pipelines_root, env, errors[], report[]}`` where each
    report entry has ``{name, status, slots: [{slot, type, connection,
    env_var, status}, ...]}``. ``status`` is ``"ok"``, ``"missing"``, or
    ``"error"``. ``env`` selects which ``pipelines/_env/<env>.yml`` overlay
    applies; defaults to ``$DLT_ENV`` then ``"dev"``.
    """
    return pipelines_cmds.doctor_pipelines(pipelines_root, env=env)


@mcp.tool
def pipelines_promote(
    name: str,
    from_env: str,
    to_env: str,
    pipelines_root: str = "pipelines",
) -> dict[str, object]:
    """Diff merged config across two envs for one pipeline (Segment 13).

    Returns ``{status, name, from_env, to_env, changes[], errors[]}``.
    ``status`` is ``"ok"``, ``"not-found"``, or ``"error"``. Each change
    entry is ``{field, from, to}`` over overlay-eligible field paths.
    Informational — never edits files.
    """
    return pipelines_cmds.promote_pipelines(name, from_env, to_env, pipelines_root)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
