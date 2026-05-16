"""MCP server tool registration + invocation tests.

Calls tools via FastMCP's in-process ``call_tool`` API — no stdio loop. The
tools themselves are thin wrappers around the CLI helpers already covered
by ``test_cli_sources.py`` / ``test_cli_pipelines.py``; this file just
proves the MCP surface is wired up.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from data_pipeline_template.mcp_server import mcp

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


def _call(tool: str, args: dict[str, object] | None = None) -> object:
    result = asyncio.run(mcp.call_tool(tool, args or {}))
    sc = result.structured_content
    # FastMCP wraps scalar / list returns under "result"; dict returns are
    # passed through verbatim.
    if isinstance(sc, dict) and set(sc.keys()) == {"result"}:
        return sc["result"]
    return sc


def test_server_registers_four_tools() -> None:
    tools = asyncio.run(mcp.list_tools())
    names = {t.name for t in tools}
    assert names == {
        "sources_list",
        "sources_describe",
        "pipelines_validate",
        "pipelines_doctor",
    }


def test_sources_list_tool() -> None:
    names = _call("sources_list")
    assert set(names) >= {"rest_api", "sql_database", "filesystem", "pg_cdc"}


def test_sources_describe_tool() -> None:
    info = _call("sources_describe", {"source_type": "sql_database"})
    assert info["env_var_template"] == "SOURCES__SQL_DATABASE__<CONNECTION>__CREDENTIALS"
    assert "tables" in info["required_config_keys"]


def test_sources_describe_unknown_raises() -> None:
    with pytest.raises(Exception) as exc:
        _call("sources_describe", {"source_type": "definitely_not_real"})
    assert "definitely_not_real" in str(exc.value)


def test_pipelines_validate_tool_ok() -> None:
    report = _call(
        "pipelines_validate",
        {"pipelines_root": str(FIXTURES / "pipelines_valid")},
    )
    assert report["status"] == "ok"
    assert set(report["pipelines"]) == {"alpha", "beta", "gamma"}


def test_pipelines_validate_tool_error() -> None:
    report = _call(
        "pipelines_validate",
        {"pipelines_root": str(FIXTURES / "pipelines_invalid")},
    )
    assert report["status"] == "error"
    assert report["errors"]


def test_pipelines_doctor_tool_structure() -> None:
    report = _call(
        "pipelines_doctor",
        {"pipelines_root": str(FIXTURES / "pipelines_valid")},
    )
    assert report["status"] in ("ok", "missing")
    assert "report" in report
    # Each report entry must carry status + slots.
    for entry in report["report"]:
        assert entry["name"]
        assert entry["status"] in ("OK", "MISSING")
        assert len(entry["slots"]) == 2  # source + destination
        for slot in entry["slots"]:
            assert slot["slot"] in ("source", "destination")
            assert slot["status"] in (
                "env",
                "secrets-toml",
                "no-creds-required",
                "MISSING",
            )
