"""Hermes tool handlers for AQL RUBICON."""

from __future__ import annotations

import json
from typing import Any, Dict

try:
    from .aql_rubicon.executor import AQLRuntime
    from .aql_rubicon.state import WorkspaceState
except ImportError:  # Direct local tests import tools.py as a top-level module.
    from aql_rubicon.executor import AQLRuntime
    from aql_rubicon.state import WorkspaceState


def _json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)


def _runtime(workspace: str | None) -> AQLRuntime:
    state = WorkspaceState(workspace or "default")
    return AQLRuntime(state)


def aql_execute(args: dict, **_: Any) -> str:
    """Execute an AQL command or script."""
    query = str(args.get("query") or "").strip()
    workspace = str(args.get("workspace") or "default").strip() or "default"
    limit = args.get("limit", 50)
    if not query:
        return _json({"ok": False, "errors": ["query is required"]})
    try:
        payload = _runtime(workspace).execute_script(query, limit=limit)
    except Exception as exc:  # Hermes tools should fail as JSON, not crash the plugin.
        payload = {
            "ok": False,
            "errors": [f"{type(exc).__name__}: {exc}"],
            "workspace": workspace,
        }
    return _json(payload)


def aql_schema(args: dict, **_: Any) -> str:
    """Inspect sources, tables, or table columns."""
    workspace = str(args.get("workspace") or "default").strip() or "default"
    target = args.get("target")
    try:
        payload = _runtime(workspace).schema(None if target is None else str(target).strip())
    except Exception as exc:
        payload = {
            "ok": False,
            "errors": [f"{type(exc).__name__}: {exc}"],
            "workspace": workspace,
        }
    return _json(payload)


def aql_register_source(args: dict, **_: Any) -> str:
    """Register or update a source."""
    workspace = str(args.get("workspace") or "default").strip() or "default"
    name = str(args.get("name") or "").strip()
    kind = str(args.get("kind") or "").strip()
    path = args.get("path")
    options = args.get("options") or {}
    try:
        payload = _runtime(workspace).register_source(name, kind, path=path, options=options)
    except Exception as exc:
        payload = {
            "ok": False,
            "errors": [f"{type(exc).__name__}: {exc}"],
            "workspace": workspace,
        }
    return _json(payload)
