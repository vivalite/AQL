"""Source wrappers that expose heterogeneous sources as relational rows."""

from __future__ import annotations

import csv
import json
import sqlite3
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from .ast import SelectItem, SourceConfig
from .predicate import compile_sql, extract_keywords, filter_rows


class WrapperError(ValueError):
    """Raised when a wrapper cannot serve a source request."""


@dataclass
class TableResult:
    columns: list[str]
    rows: list[dict[str, Any]]
    provenance: list[dict[str, Any]] = field(default_factory=list)
    trace: list[dict[str, Any]] = field(default_factory=list)
    truncated: bool = False
    warnings: list[str] = field(default_factory=list)


class SourceWrapper:
    def __init__(self, config: SourceConfig) -> None:
        self.config = config

    def list_tables(self) -> list[str]:
        raise NotImplementedError

    def schema(self, table: str) -> list[dict[str, str]]:
        raise NotImplementedError

    def query(self, table: str, columns: list[str], where: str | None, limit: int) -> TableResult:
        rows = filter_rows(self._all_rows(table), where)
        projected, output_columns = project_rows(rows, columns)
        truncated = limit > 0 and len(projected) > limit
        if limit > 0:
            projected = projected[:limit]
        return TableResult(
            columns=output_columns,
            rows=projected,
            provenance=[{"source": self.config.name, "kind": self.config.kind, "table": table}],
            trace=[
                {
                    "source": self.config.name,
                    "table": table,
                    "predicate": where,
                    "rows_after_filter": len(rows),
                    "rows_returned": len(projected),
                    "truncated": truncated,
                }
            ],
            truncated=truncated,
        )

    def query_aggregate(self, table: str, columns: tuple[SelectItem, ...], where: str | None) -> TableResult | None:
        return None

    def _all_rows(self, table: str) -> list[dict[str, Any]]:
        raise NotImplementedError


class SQLiteWrapper(SourceWrapper):
    def _connect(self) -> sqlite3.Connection:
        path = self.config.path
        if not path:
            raise WrapperError(f"sqlite source {self.config.name} requires a path")
        if not Path(path).exists():
            raise WrapperError(f"sqlite database not found: {path}")
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        return conn

    def list_tables(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
        return [row["name"] for row in rows]

    def schema(self, table: str) -> list[dict[str, str]]:
        with self._connect() as conn:
            rows = conn.execute(f"PRAGMA table_info({_quote_identifier(table)})").fetchall()
        if not rows:
            raise WrapperError(f"unknown sqlite table: {table}")
        return [{"name": row["name"], "type": row["type"] or "TEXT"} for row in rows]

    def _all_rows(self, table: str) -> list[dict[str, Any]]:
        if table not in self.list_tables():
            raise WrapperError(f"unknown sqlite table: {table}")
        with self._connect() as conn:
            rows = conn.execute(f"SELECT * FROM {_quote_identifier(table)}").fetchall()
        return [dict(row) for row in rows]

    def query(self, table: str, columns: list[str], where: str | None, limit: int) -> TableResult:
        schema = self.schema(table)
        schema_columns = [column["name"] for column in schema]
        selected_columns = schema_columns if columns == ["*"] else columns
        warnings: list[str] = []
        if not all(_has_column(column, schema_columns) for column in selected_columns):
            fallback = super().query(table, columns, where, limit)
            fallback.warnings.append("sqlite pushdown skipped: unknown projected column")
            fallback.trace.append({"operation": "pushdown", "source": self.config.name, "supported": False})
            return fallback
        predicate = compile_sql(where, schema_columns)
        if not predicate.supported:
            fallback = super().query(table, columns, where, limit)
            fallback.warnings.extend(predicate.warnings)
            fallback.trace.append(
                {
                    "operation": "pushdown",
                    "source": self.config.name,
                    "supported": False,
                    "warnings": list(predicate.warnings),
                }
            )
            return fallback

        sql_columns = "*" if columns == ["*"] else ", ".join(_quote_identifier(_resolve_column_name(column, schema_columns)) for column in columns)
        sql = f"SELECT {sql_columns} FROM {_quote_identifier(table)}"
        params = list(predicate.params)
        if where:
            sql += f" WHERE {predicate.sql}"
        if limit > 0:
            sql += " LIMIT ?"
            params.append(limit + 1)
        with self._connect() as conn:
            rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
        truncated = limit > 0 and len(rows) > limit
        if truncated:
            rows = rows[:limit]
        if columns == ["*"]:
            output_columns = schema_columns
        else:
            output_columns = [_resolve_column_name(column, schema_columns) for column in columns]
        return TableResult(
            columns=output_columns,
            rows=rows,
            provenance=[{"source": self.config.name, "kind": self.config.kind, "table": table}],
            trace=[
                {
                    "source": self.config.name,
                    "table": table,
                    "predicate": where,
                    "rows_returned": len(rows),
                    "truncated": truncated,
                    "pushdown": True,
                    "sql": sql,
                }
            ],
            truncated=truncated,
            warnings=warnings,
        )

    def query_aggregate(self, table: str, columns: tuple[SelectItem, ...], where: str | None) -> TableResult | None:
        schema = self.schema(table)
        schema_columns = [column["name"] for column in schema]
        predicate = compile_sql(where, schema_columns)
        if not predicate.supported:
            return None
        expressions: list[str] = []
        output_columns: list[str] = []
        for item in columns:
            if not item.aggregate:
                return None
            if item.column == "*":
                if item.aggregate != "count":
                    return None
                expression = "COUNT(*)"
            else:
                if not _has_column(item.column, schema_columns):
                    return None
                expression = f"{item.aggregate.upper()}({_quote_identifier(_resolve_column_name(item.column, schema_columns))})"
            expressions.append(f"{expression} AS {_quote_alias(item.output_name)}")
            output_columns.append(item.output_name)
        sql = f"SELECT {', '.join(expressions)} FROM {_quote_identifier(table)}"
        params = list(predicate.params)
        if where:
            sql += f" WHERE {predicate.sql}"
        with self._connect() as conn:
            row = dict(conn.execute(sql, params).fetchone())
        return TableResult(
            columns=output_columns,
            rows=[row],
            provenance=[{"source": self.config.name, "kind": self.config.kind, "table": table}],
            trace=[
                {
                    "source": self.config.name,
                    "table": table,
                    "predicate": where,
                    "rows_returned": 1,
                    "pushdown": True,
                    "aggregate_pushdown": True,
                    "sql": sql,
                }
            ],
        )


class CSVDirWrapper(SourceWrapper):
    def _root(self) -> Path:
        if not self.config.path:
            raise WrapperError(f"csv_dir source {self.config.name} requires a path")
        root = Path(self.config.path)
        if not root.exists() or not root.is_dir():
            raise WrapperError(f"csv_dir path not found: {root}")
        return root

    def list_tables(self) -> list[str]:
        return sorted(path.stem for path in self._root().glob("*.csv"))

    def schema(self, table: str) -> list[dict[str, str]]:
        rows = self._all_rows(table)
        columns = _columns_from_rows(rows)
        return [{"name": column, "type": "TEXT"} for column in columns]

    def _all_rows(self, table: str) -> list[dict[str, Any]]:
        path = self._root() / f"{table}.csv"
        if not path.exists():
            raise WrapperError(f"unknown csv table: {table}")
        with path.open(newline="", encoding="utf-8") as handle:
            return [dict(row) for row in csv.DictReader(handle)]


class JSONDirWrapper(SourceWrapper):
    def _root(self) -> Path:
        if not self.config.path:
            raise WrapperError(f"json_dir source {self.config.name} requires a path")
        root = Path(self.config.path)
        if not root.exists() or not root.is_dir():
            raise WrapperError(f"json_dir path not found: {root}")
        return root

    def list_tables(self) -> list[str]:
        return sorted(path.stem for path in self._root().glob("*.json"))

    def schema(self, table: str) -> list[dict[str, str]]:
        rows = self._all_rows(table)
        return [{"name": column, "type": _infer_type([row.get(column) for row in rows])} for column in _columns_from_rows(rows)]

    def _all_rows(self, table: str) -> list[dict[str, Any]]:
        path = self._root() / f"{table}.json"
        if not path.exists():
            raise WrapperError(f"unknown json table: {table}")
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict) and isinstance(raw.get("rows"), list):
            raw = raw["rows"]
        if not isinstance(raw, list):
            raise WrapperError(f"json table must be a list of objects: {table}")
        rows: list[dict[str, Any]] = []
        for item in raw:
            if isinstance(item, dict):
                rows.append(dict(item))
        return rows


class WikipediaWrapper(SourceWrapper):
    TABLES = ["pages"]

    def list_tables(self) -> list[str]:
        return list(self.TABLES)

    def schema(self, table: str) -> list[dict[str, str]]:
        if table not in self.TABLES:
            raise WrapperError(f"unknown wikipedia table: {table}")
        return [
            {"name": "title", "type": "TEXT"},
            {"name": "url", "type": "TEXT"},
            {"name": "snippet", "type": "TEXT"},
            {"name": "text", "type": "TEXT"},
            {"name": "categories", "type": "TEXT"},
        ]

    def query(self, table: str, columns: list[str], where: str | None, limit: int) -> TableResult:
        if table not in self.TABLES:
            raise WrapperError(f"unknown wikipedia table: {table}")
        rows = self._search(where, limit=max(1, min(limit, 50)))
        projected, output_columns = project_rows(rows, columns)
        return TableResult(
            columns=output_columns,
            rows=projected,
            provenance=[{"source": self.config.name, "kind": self.config.kind, "table": table}],
            trace=[
                {
                    "source": self.config.name,
                    "table": table,
                    "predicate": where,
                    "external_api": "mediawiki_opensearch",
                    "rows_returned": len(projected),
                }
            ],
        )

    def _all_rows(self, table: str) -> list[dict[str, Any]]:
        return self._search(None, limit=10)

    def _search(self, where: str | None, *, limit: int) -> list[dict[str, Any]]:
        keywords = extract_keywords(where)
        search = " ".join(keywords) or "university"
        params = urllib.parse.urlencode(
            {
                "action": "opensearch",
                "namespace": "0",
                "search": search,
                "limit": str(limit),
                "format": "json",
            }
        )
        url = f"https://en.wikipedia.org/w/api.php?{params}"
        request = urllib.request.Request(url, headers={"User-Agent": "aql-rubicon-hermes-plugin/0.1"})
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
        titles = payload[1] if len(payload) > 1 else []
        snippets = payload[2] if len(payload) > 2 else []
        urls = payload[3] if len(payload) > 3 else []
        rows: list[dict[str, Any]] = []
        for index, title in enumerate(titles):
            rows.append(
                {
                    "title": title,
                    "url": urls[index] if index < len(urls) else "",
                    "snippet": snippets[index] if index < len(snippets) else "",
                    "text": snippets[index] if index < len(snippets) else "",
                    "categories": "",
                }
            )
        return rows


class SavedTableWrapper(SourceWrapper):
    def __init__(self, name: str, payload: dict[str, Any]) -> None:
        super().__init__(SourceConfig(name="saved", kind="json_dir", path=None, options={}))
        self.name = name
        self.payload = payload

    def list_tables(self) -> list[str]:
        return [self.name]

    def schema(self, table: str) -> list[dict[str, str]]:
        if table != self.name:
            raise WrapperError(f"unknown saved table: {table}")
        columns = self.payload.get("columns") or _columns_from_rows(self.payload.get("rows") or [])
        return [{"name": column, "type": "TEXT"} for column in columns]

    def _all_rows(self, table: str) -> list[dict[str, Any]]:
        if table != self.name:
            raise WrapperError(f"unknown saved table: {table}")
        rows = self.payload.get("rows") or []
        return [dict(row) for row in rows if isinstance(row, dict)]


def make_wrapper(config: SourceConfig) -> SourceWrapper:
    if config.kind == "sqlite":
        return SQLiteWrapper(config)
    if config.kind == "csv_dir":
        return CSVDirWrapper(config)
    if config.kind == "json_dir":
        return JSONDirWrapper(config)
    if config.kind == "wikipedia":
        return WikipediaWrapper(config)
    raise WrapperError(f"unsupported source kind: {config.kind}")


def project_rows(rows: Iterable[dict[str, Any]], columns: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    materialized = list(rows)
    if columns == ["*"]:
        output_columns = _columns_from_rows(materialized)
    else:
        output_columns = columns
    projected: list[dict[str, Any]] = []
    for row in materialized:
        if output_columns:
            projected.append({column: _lookup_column(row, column) for column in output_columns})
        else:
            projected.append(dict(row))
    return projected, output_columns


def _lookup_column(row: dict[str, Any], column: str) -> Any:
    if column in row:
        return row[column]
    lowered = column.lower()
    for key, value in row.items():
        if key.lower() == lowered or key.lower().endswith(f".{lowered}"):
            return value
    return None


def _columns_from_rows(rows: Iterable[dict[str, Any]]) -> list[str]:
    columns: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for column in row:
            if column not in seen:
                seen.add(column)
                columns.append(column)
    return columns


def _infer_type(values: list[Any]) -> str:
    non_null = [value for value in values if value not in (None, "")]
    if not non_null:
        return "TEXT"
    if all(isinstance(value, bool) for value in non_null):
        return "BOOLEAN"
    if all(isinstance(value, int) and not isinstance(value, bool) for value in non_null):
        return "INTEGER"
    if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in non_null):
        return "REAL"
    return "TEXT"


def _quote_identifier(identifier: str) -> str:
    if not identifier.replace("_", "").isalnum():
        raise WrapperError(f"invalid sqlite identifier: {identifier}")
    return f'"{identifier}"'


def _quote_alias(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _has_column(column: str, columns: list[str]) -> bool:
    lowered = column.lower()
    return any(existing.lower() == lowered for existing in columns)


def _resolve_column_name(column: str, columns: list[str]) -> str:
    lowered = column.lower()
    for existing in columns:
        if existing.lower() == lowered:
            return existing
    return column
