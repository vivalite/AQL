from __future__ import annotations

import importlib.util
from pathlib import Path


class FakeCtx:
    def __init__(self) -> None:
        self.tools: dict[str, dict] = {}
        self.skills: dict[str, Path] = {}

    def register_tool(self, *, name, toolset, schema, handler, **kwargs) -> None:
        self.tools[name] = {"toolset": toolset, "schema": schema, "handler": handler, "kwargs": kwargs}

    def register_skill(self, name, path) -> None:
        self.skills[name] = Path(path)


def test_plugin_registers_tools_and_skill() -> None:
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "aql_rubicon_plugin",
        root / "__init__.py",
        submodule_search_locations=[str(root)],
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    ctx = FakeCtx()
    module.register(ctx)

    assert set(ctx.tools) == {"aql_execute", "aql_schema", "aql_register_source"}
    assert {payload["toolset"] for payload in ctx.tools.values()} == {"aql"}
    assert "aql-rubicon" in ctx.skills
    assert ctx.skills["aql-rubicon"].exists()

