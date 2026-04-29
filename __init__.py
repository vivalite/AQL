"""Hermes plugin registration for AQL RUBICON."""

from __future__ import annotations

from pathlib import Path

try:
    from . import schemas, tools
except ImportError:  # Direct import smoke tests and some plugin loaders use module mode.
    import schemas  # type: ignore[no-redef]
    import tools  # type: ignore[no-redef]


def register(ctx) -> None:
    """Register AQL tools and bundled skills with Hermes."""
    ctx.register_tool(
        name="aql_execute",
        toolset="aql",
        schema=schemas.AQL_EXECUTE,
        handler=tools.aql_execute,
    )
    ctx.register_tool(
        name="aql_schema",
        toolset="aql",
        schema=schemas.AQL_SCHEMA,
        handler=tools.aql_schema,
    )
    ctx.register_tool(
        name="aql_register_source",
        toolset="aql",
        schema=schemas.AQL_REGISTER_SOURCE,
        handler=tools.aql_register_source,
    )

    skills_dir = Path(__file__).parent / "skills"
    skill_md = skills_dir / "aql-rubicon" / "SKILL.md"
    if skill_md.exists() and hasattr(ctx, "register_skill"):
        ctx.register_skill("aql-rubicon", skill_md)
