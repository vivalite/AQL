"""Workspace state for AQL source configs and saved intermediate tables."""

from __future__ import annotations

import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Any

from .ast import SourceConfig


class StateError(ValueError):
    """Raised for invalid workspace state operations."""


_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$")


class WorkspaceState:
    """Persistent workspace state stored under ~/.hermes/aql-rubicon."""

    def __init__(self, workspace: str = "default") -> None:
        if not _NAME_RE.match(workspace):
            raise StateError("workspace must contain only letters, numbers, underscores, and hyphens")
        self.workspace = workspace
        self.root = _state_root() / workspace
        self.tables_dir = self.root / "tables"
        self.trace_dir = self.root / "traces"
        self.root.mkdir(parents=True, exist_ok=True)
        self.tables_dir.mkdir(parents=True, exist_ok=True)
        self.trace_dir.mkdir(parents=True, exist_ok=True)
        self._ensure_defaults()

    @property
    def sources_file(self) -> Path:
        return self.root / "sources.json"

    def list_sources(self) -> dict[str, SourceConfig]:
        payload = _read_json(self.sources_file, default={"sources": {}})
        sources: dict[str, SourceConfig] = {}
        for name, raw in payload.get("sources", {}).items():
            sources[name] = SourceConfig(
                name=name,
                kind=raw["kind"],
                path=raw.get("path"),
                options=raw.get("options") or {},
                enabled=bool(raw.get("enabled", True)),
            )
        return sources

    def save_source(self, source: SourceConfig) -> None:
        _validate_name(source.name, label="source")
        payload = _read_json(self.sources_file, default={"sources": {}})
        payload.setdefault("sources", {})[source.name] = {
            "kind": source.kind,
            "path": source.path,
            "options": source.options or {},
            "enabled": source.enabled,
        }
        _write_json(self.sources_file, payload)

    def delete_source(self, name: str) -> bool:
        _validate_name(name, label="source")
        payload = _read_json(self.sources_file, default={"sources": {}})
        if name not in payload.get("sources", {}):
            return False
        del payload["sources"][name]
        _write_json(self.sources_file, payload)
        return True

    def set_source_enabled(self, name: str, enabled: bool) -> SourceConfig:
        _validate_name(name, label="source")
        sources = self.list_sources()
        if name not in sources:
            raise StateError(f"source not found: {name}")
        source = sources[name]
        updated = SourceConfig(
            name=source.name,
            kind=source.kind,
            path=source.path,
            options=source.options or {},
            enabled=enabled,
        )
        self.save_source(updated)
        return updated

    def list_saved_tables(self) -> list[str]:
        return sorted(path.stem for path in self.tables_dir.glob("*.json"))

    def load_table(self, table: str) -> dict[str, Any]:
        _validate_name(table, label="table")
        path = self.tables_dir / f"{table}.json"
        if not path.exists():
            raise StateError(f"saved table not found: {table}")
        return _read_json(path, default={})

    def save_table(self, table: str, columns: list[str], rows: list[dict[str, Any]], provenance: list[dict[str, Any]]) -> None:
        _validate_name(table, label="table")
        payload = {
            "table": table,
            "columns": columns,
            "rows": rows,
            "provenance": provenance,
        }
        _write_json(self.tables_dir / f"{table}.json", payload)

    def delete_table(self, table: str) -> bool:
        _validate_name(table, label="table")
        path = self.tables_dir / f"{table}.json"
        if not path.exists():
            return False
        path.unlink()
        return True

    def save_trace(self, trace: dict[str, Any]) -> Path:
        existing = sorted(self.trace_dir.glob("trace-*.json"))
        next_id = len(existing) + 1
        path = self.trace_dir / f"trace-{next_id:05d}.json"
        _write_json(path, trace)
        return path

    def _ensure_defaults(self) -> None:
        if self.sources_file.exists():
            return
        demo_sqlite = self.root / "demo" / "university.sqlite"
        _ensure_demo_sqlite(demo_sqlite)
        demo_lab = _repo_root() / "data" / "demo" / "lab"
        payload = {
            "sources": {
                "demo_university": {
                    "kind": "sqlite",
                    "path": str(demo_sqlite),
                    "options": {"description": "Bundled university data warehouse demo"},
                    "enabled": True,
                },
                "demo_lab": {
                    "kind": "json_dir",
                    "path": str(demo_lab),
                    "options": {"description": "Bundled research lab website demo"},
                    "enabled": True,
                },
                "wikipedia": {
                    "kind": "wikipedia",
                    "path": None,
                    "options": {"description": "Public MediaWiki search wrapper"},
                    "enabled": True,
                },
            }
        }
        _write_json(self.sources_file, payload)


def _state_root() -> Path:
    raw = os.environ.get("AQL_RUBICON_HOME")
    if raw:
        return Path(raw).expanduser()
    hermes_home = os.environ.get("HERMES_HOME")
    if hermes_home:
        return Path(hermes_home).expanduser() / "aql-rubicon"
    return Path.home() / ".hermes" / "aql-rubicon"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _ensure_demo_sqlite(path: Path) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    sql_path = _repo_root() / "data" / "demo" / "university.sql"
    with sqlite3.connect(path) as conn:
        conn.executescript(sql_path.read_text(encoding="utf-8"))


def _validate_name(name: str, *, label: str) -> None:
    if not _NAME_RE.match(name):
        raise StateError(f"{label} name must contain only letters, numbers, underscores, and hyphens")


def _read_json(path: Path, *, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
