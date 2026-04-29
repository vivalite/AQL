"""AQL execution runtime."""

from __future__ import annotations

import math
from typing import Any

from .ast import (
    Command,
    DeleteCommand,
    ExplainCommand,
    FindQuery,
    JoinCondition,
    JoinQuery,
    OutputCommand,
    SaveCommand,
    SchemaCommand,
    SelectItem,
    SourceConfig,
)
from .parser import parse, split_script
from .predicate import compile_sql, predicate_plan
from .state import WorkspaceState
from .wrappers import SavedTableWrapper, TableResult, make_wrapper


class AQLError(ValueError):
    """Raised for AQL execution errors."""


class AQLRuntime:
    """Execute parsed AQL commands against a workspace."""

    def __init__(self, state: WorkspaceState) -> None:
        self.state = state

    def execute_script(self, script: str, *, limit: Any = 50) -> dict[str, Any]:
        row_limit = _coerce_limit(limit)
        raw_commands = split_script(script)
        if not raw_commands:
            return {"ok": False, "workspace": self.state.workspace, "errors": ["empty AQL script"]}
        parsed_commands = [parse(raw) for raw in raw_commands]
        results: list[dict[str, Any]] = []
        errors: list[str] = []
        for raw, command in zip(raw_commands, parsed_commands):
            try:
                result = self.execute(command, limit=row_limit)
                result["command"] = raw
                results.append(result)
                if not result.get("ok", False):
                    errors.extend(result.get("errors", []))
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                errors.append(error)
                results.append({"ok": False, "command": raw, "errors": [error]})
        payload: dict[str, Any] = {
            "ok": not errors,
            "workspace": self.state.workspace,
            "mode": "compiled" if len(raw_commands) > 1 else "interactive",
            "results": results,
            "errors": errors,
        }
        if len(results) == 1:
            payload.update({key: value for key, value in results[0].items() if key not in {"ok", "errors"}})
        if not all(isinstance(command, ExplainCommand) for command in parsed_commands):
            trace_path = self.state.save_trace(payload)
            payload["trace_path"] = str(trace_path)
        return payload

    def explain_script(self, script: str, *, limit: Any = 50) -> dict[str, Any]:
        row_limit = _coerce_limit(limit)
        raw_commands = split_script(script)
        if not raw_commands:
            return {"ok": False, "workspace": self.state.workspace, "errors": ["empty AQL script"]}
        results: list[dict[str, Any]] = []
        errors: list[str] = []
        for raw in raw_commands:
            try:
                result = self.explain(parse(raw), limit=row_limit)
                result["command"] = raw
                results.append(result)
                if not result.get("ok", False):
                    errors.extend(result.get("errors", []))
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                errors.append(error)
                results.append({"ok": False, "command": raw, "errors": [error]})
        return {
            "ok": not errors,
            "workspace": self.state.workspace,
            "mode": "explain",
            "results": results,
            "errors": errors,
        }

    def execute(self, command: Command, *, limit: int = 50) -> dict[str, Any]:
        if isinstance(command, SchemaCommand):
            return self.schema(command.target)
        if isinstance(command, ExplainCommand):
            return self.explain(command.command, limit=limit)
        if isinstance(command, FindQuery):
            result = self._execute_find(command, limit=limit)
            return _result_payload(result, workspace=self.state.workspace)
        if isinstance(command, JoinQuery):
            result = self._execute_join(command, limit=limit)
            return _result_payload(result, workspace=self.state.workspace)
        if isinstance(command, SaveCommand):
            result = self._execute_query(command.query, limit=0)
            self.state.save_table(command.table, result.columns, result.rows, result.provenance)
            payload = _result_payload(result, workspace=self.state.workspace)
            payload["saved_table"] = command.table
            payload["message"] = f"saved {len(result.rows)} row(s) as {command.table}"
            return payload
        if isinstance(command, OutputCommand):
            payload = self.state.load_table(command.table)
            result = TableResult(
                columns=list(payload.get("columns") or []),
                rows=list(payload.get("rows") or []),
                provenance=list(payload.get("provenance") or [{"source": "saved", "table": command.table}]),
                trace=[{"operation": "output", "table": command.table, "rows_returned": len(payload.get("rows") or [])}],
            )
            return _result_payload(result, workspace=self.state.workspace)
        if isinstance(command, DeleteCommand):
            deleted = self.state.delete_table(command.table)
            return {
                "ok": True,
                "workspace": self.state.workspace,
                "deleted_table": command.table,
                "deleted": deleted,
                "message": f"deleted {command.table}" if deleted else f"table did not exist: {command.table}",
            }
        raise AQLError(f"unsupported command: {type(command).__name__}")

    def schema(self, target: str | None = None) -> dict[str, Any]:
        target = (target or "").strip()
        sources = self.state.list_sources()
        saved = self.state.list_saved_tables()
        if not target:
            return {
                "ok": True,
                "workspace": self.state.workspace,
                "sources": [
                    {
                        "name": source.name,
                        "kind": source.kind,
                        "path": source.path,
                        "options": source.options or {},
                        "enabled": source.enabled,
                    }
                    for source in sources.values()
                ],
                "saved_tables": saved,
            }
        if target in sources:
            wrapper = make_wrapper(sources[target])
            return {
                "ok": True,
                "workspace": self.state.workspace,
                "source": target,
                "kind": sources[target].kind,
                "enabled": sources[target].enabled,
                "tables": [
                    {"name": table, "qualified_name": f"{target}.{table}"}
                    for table in wrapper.list_tables()
                ],
            }
        wrapper, table, source_name = self._resolve_table(target)
        return {
            "ok": True,
            "workspace": self.state.workspace,
            "source": source_name,
            "table": table,
            "columns": wrapper.schema(table),
        }

    def register_source(self, name: str, kind: str, *, path: Any = None, options: Any = None) -> dict[str, Any]:
        name = name.strip()
        kind = kind.strip()
        if kind not in {"sqlite", "csv_dir", "json_dir", "wikipedia"}:
            raise AQLError("kind must be one of: sqlite, csv_dir, json_dir, wikipedia")
        normalized_path = None if path in (None, "") else str(path)
        if kind != "wikipedia" and not normalized_path:
            raise AQLError(f"{kind} sources require a path")
        source = SourceConfig(
            name=name,
            kind=kind,  # type: ignore[arg-type]
            path=normalized_path,
            options=options if isinstance(options, dict) else {},
            enabled=True,
        )
        self.state.save_source(source)
        wrapper = make_wrapper(source)
        return {
            "ok": True,
            "workspace": self.state.workspace,
            "source": {
                "name": source.name,
                "kind": source.kind,
                "path": source.path,
                "options": source.options or {},
                "enabled": source.enabled,
            },
            "tables": wrapper.list_tables(),
        }

    def sources(self, action: str = "list", *, name: str | None = None) -> dict[str, Any]:
        action = (action or "list").strip().lower()
        sources = self.state.list_sources()
        if action in {"list", "ls"}:
            return {
                "ok": True,
                "workspace": self.state.workspace,
                "sources": [_source_payload(source) for source in sources.values()],
            }
        if not name:
            raise AQLError(f"name is required for source action: {action}")
        if action == "show":
            if name not in sources:
                raise AQLError(f"source not found: {name}")
            source = sources[name]
            payload = _source_payload(source)
            payload["tables"] = _safe_tables(source)
            return {"ok": True, "workspace": self.state.workspace, "source": payload}
        if action == "status":
            if name not in sources:
                raise AQLError(f"source not found: {name}")
            return {"ok": True, "workspace": self.state.workspace, "source": _source_status(sources[name])}
        if action == "enable":
            return {
                "ok": True,
                "workspace": self.state.workspace,
                "source": _source_payload(self.state.set_source_enabled(name, True)),
            }
        if action == "disable":
            return {
                "ok": True,
                "workspace": self.state.workspace,
                "source": _source_payload(self.state.set_source_enabled(name, False)),
            }
        if action in {"delete", "remove", "rm"}:
            return {
                "ok": True,
                "workspace": self.state.workspace,
                "deleted_source": name,
                "deleted": self.state.delete_source(name),
            }
        raise AQLError("action must be one of: list, show, status, enable, disable, delete")

    def explain(self, command: Command, *, limit: int = 50) -> dict[str, Any]:
        if isinstance(command, ExplainCommand):
            return self.explain(command.command, limit=limit)
        try:
            plan = self._explain_command(command, limit=limit)
            return {"ok": True, "workspace": self.state.workspace, "plan": plan, "errors": []}
        except Exception as exc:
            return {
                "ok": False,
                "workspace": self.state.workspace,
                "plan": None,
                "errors": [f"{type(exc).__name__}: {exc}"],
            }

    def _execute_query(self, query: FindQuery | JoinQuery, *, limit: int) -> TableResult:
        if isinstance(query, FindQuery):
            return self._execute_find(query, limit=limit)
        return self._execute_join(query, limit=limit)

    def _execute_find(self, query: FindQuery, *, limit: int) -> TableResult:
        wrapper, table, source_name = self._resolve_table(query.table)
        if _has_aggregate(query.columns):
            pushed = wrapper.query_aggregate(table, query.columns, query.where)
            if pushed is not None:
                pushed.trace.append({"operation": "aggregate", "expressions": [item.raw for item in query.columns]})
                return pushed
            base = wrapper.query(table, ["*"], query.where, limit=0)
            result = _aggregate_rows(base.rows, query.columns)
            result.provenance = base.provenance
            result.trace = base.trace + [{"operation": "aggregate", "expressions": [item.raw for item in query.columns]}]
            result.warnings = base.warnings
            return result
        raw_columns = _selected_columns(query.columns)
        base = wrapper.query(table, raw_columns, query.where, limit=limit)
        if _has_alias(query.columns):
            base = _apply_aliases(base, query.columns)
        base.trace.append(
            {
                "operation": "find",
                "source": source_name,
                "table": table,
                "columns": [item.raw for item in query.columns],
                "where": query.where,
            }
        )
        return base

    def _execute_join(self, query: JoinQuery, *, limit: int) -> TableResult:
        current = self._execute_find(query.first, limit=0)
        join_trace: list[dict[str, Any]] = []
        for step in query.steps:
            right = self._execute_find(step.query, limit=0)
            current, trace = _join_results(current, right, step.conditions, right_table=step.query.table, limit=0)
            join_trace.append(trace)
        if limit > 0:
            current.truncated = len(current.rows) > limit or current.truncated
            current.rows = current.rows[:limit]
        current.trace.extend(join_trace)
        current.trace.append({"operation": "join", "parts": len(query.parts), "rows_returned": len(current.rows)})
        return current

    def _resolve_table(self, table_ref: str):
        sources = self.state.list_sources()
        saved = self.state.list_saved_tables()
        if "." in table_ref:
            source_name, table = table_ref.split(".", 1)
            if source_name in sources:
                if not sources[source_name].enabled:
                    raise AQLError(f"source is disabled: {source_name}")
                return make_wrapper(sources[source_name]), table, source_name
        if table_ref in saved:
            return SavedTableWrapper(table_ref, self.state.load_table(table_ref)), table_ref, "saved"

        candidates = []
        for source in sources.values():
            if not source.enabled:
                continue
            wrapper = make_wrapper(source)
            try:
                if table_ref in wrapper.list_tables():
                    candidates.append((wrapper, table_ref, source.name))
            except Exception:
                continue
        if table_ref in saved:
            candidates.append((SavedTableWrapper(table_ref, self.state.load_table(table_ref)), table_ref, "saved"))
        if not candidates:
            raise AQLError(f"table not found: {table_ref}")
        if len(candidates) > 1:
            names = ", ".join(f"{source}.{table}" if source != "saved" else table for _, table, source in candidates)
            raise AQLError(f"ambiguous table '{table_ref}', use one of: {names}")
        return candidates[0]

    def _explain_command(self, command: Command, *, limit: int) -> dict[str, Any]:
        if isinstance(command, FindQuery):
            return {"type": "find", "limit": limit, "query": self._explain_find(command)}
        if isinstance(command, JoinQuery):
            return {
                "type": "join",
                "limit": limit,
                "first": self._explain_find(command.first),
                "steps": [
                    {
                        "query": self._explain_find(step.query),
                        "conditions": [{"left": cond.left, "right": cond.right} for cond in step.conditions],
                        "join_type": "explicit" if step.conditions else "natural",
                    }
                    for step in command.steps
                ],
            }
        if isinstance(command, SaveCommand):
            return {"type": "save", "table": command.table, "query": self._explain_command(command.query, limit=limit)}
        if isinstance(command, OutputCommand):
            return {"type": "output", "table": command.table, "exists": command.table in self.state.list_saved_tables()}
        if isinstance(command, DeleteCommand):
            return {"type": "delete", "table": command.table, "exists": command.table in self.state.list_saved_tables()}
        if isinstance(command, SchemaCommand):
            return {"type": "schema", "target": command.target}
        raise AQLError(f"unsupported command: {type(command).__name__}")

    def _explain_find(self, query: FindQuery) -> dict[str, Any]:
        wrapper, table, source_name = self._resolve_table(query.table)
        schema = wrapper.schema(table)
        schema_columns = [column["name"] for column in schema]
        pushdown = False
        pushdown_warnings: list[str] = []
        if getattr(wrapper, "config", None) and wrapper.config.kind == "sqlite":
            compiled = compile_sql(query.where, schema_columns)
            pushdown = compiled.supported
            pushdown_warnings = list(compiled.warnings)
        return {
            "table": query.table,
            "resolved_source": source_name,
            "resolved_table": table,
            "columns": [item.raw for item in query.columns],
            "aggregates": [item.raw for item in query.columns if item.aggregate],
            "predicate": query.where,
            "predicate_plan": predicate_plan(query.where),
            "pushdown": pushdown,
            "pushdown_warnings": pushdown_warnings,
            "schema_columns": schema_columns,
        }


def _result_payload(result: TableResult, *, workspace: str) -> dict[str, Any]:
    return {
        "ok": True,
        "workspace": workspace,
        "columns": result.columns,
        "rows": result.rows,
        "row_count": len(result.rows),
        "provenance": result.provenance,
        "trace": result.trace,
        "truncated": result.truncated,
        "warnings": result.warnings,
        "errors": [],
    }


def _coerce_limit(raw: Any) -> int:
    try:
        value = int(raw)
    except Exception:
        value = 50
    return max(1, min(500, value))


def _has_aggregate(columns: tuple[SelectItem, ...]) -> bool:
    return any(item.aggregate for item in columns)


def _has_alias(columns: tuple[SelectItem, ...]) -> bool:
    return any(item.alias for item in columns)


def _selected_columns(columns: tuple[SelectItem, ...]) -> list[str]:
    if len(columns) == 1 and columns[0].column == "*":
        return ["*"]
    return [item.column for item in columns]


def _apply_aliases(result: TableResult, columns: tuple[SelectItem, ...]) -> TableResult:
    mapping = {item.column: item.output_name for item in columns}
    output_columns = [item.output_name for item in columns]
    rows: list[dict[str, Any]] = []
    for row in result.rows:
        rows.append({mapping.get(item.column, item.column): row.get(item.column) for item in columns})
    result.columns = output_columns
    result.rows = rows
    return result


def _aggregate_rows(rows: list[dict[str, Any]], columns: tuple[SelectItem, ...]) -> TableResult:
    output: dict[str, Any] = {}
    for item in columns:
        if not item.aggregate:
            continue
        values = [row.get(item.column) for row in rows] if item.column != "*" else rows
        output[item.output_name] = _aggregate(item.aggregate, values)
    return TableResult(columns=list(output), rows=[output])


def _aggregate(name: str, values: list[Any]) -> Any:
    if name == "count":
        return len([value for value in values if value not in (None, "")])
    numeric = [_to_float(value) for value in values]
    numeric = [value for value in numeric if value is not None]
    if name == "sum":
        return sum(numeric)
    if name == "avg":
        return sum(numeric) / len(numeric) if numeric else None
    if name == "min":
        return min(values) if values else None
    if name == "max":
        return max(values) if values else None
    raise AQLError(f"unsupported aggregate: {name}")


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        parsed = float(str(value).replace(",", ""))
        if math.isnan(parsed):
            return None
        return parsed
    except ValueError:
        return None


def _join_results(
    left: TableResult,
    right: TableResult,
    conditions: tuple[JoinCondition, ...],
    *,
    right_table: str,
    limit: int,
) -> tuple[TableResult, dict[str, Any]]:
    if conditions:
        return _explicit_join(left, right, conditions, right_table=right_table, limit=limit)
    return _natural_join(left, right, limit=limit)


def _natural_join(left: TableResult, right: TableResult, *, limit: int) -> tuple[TableResult, dict[str, Any]]:
    common = [column for column in left.columns if column in set(right.columns)]
    rows: list[dict[str, Any]] = []
    for left_row in left.rows:
        for right_row in right.rows:
            if _joinable(left_row, right_row, common):
                merged = dict(left_row)
                for column, value in right_row.items():
                    if column not in merged:
                        merged[column] = value
                rows.append(merged)
                if limit and len(rows) >= limit:
                    break
        if limit and len(rows) >= limit:
            break
    columns = list(left.columns) + [column for column in right.columns if column not in left.columns]
    trace = {
        "operation": "natural_join",
        "join_columns": common,
        "left_rows": len(left.rows),
        "right_rows": len(right.rows),
        "rows_returned": len(rows),
        "join_type": "natural" if common else "cross",
    }
    return (
        TableResult(
            columns=columns,
            rows=rows,
            provenance=left.provenance + right.provenance,
            trace=left.trace + right.trace,
            truncated=left.truncated or right.truncated,
            warnings=left.warnings + right.warnings,
        ),
        trace,
    )


def _joinable(left: dict[str, Any], right: dict[str, Any], common: list[str]) -> bool:
    if not common:
        return True
    return all(str(left.get(column)) == str(right.get(column)) for column in common)


def _explicit_join(
    left: TableResult,
    right: TableResult,
    conditions: tuple[JoinCondition, ...],
    *,
    right_table: str,
    limit: int,
) -> tuple[TableResult, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    right_join_columns = {condition.right for condition in conditions}
    for left_row in left.rows:
        for right_row in right.rows:
            if _explicit_joinable(left_row, right_row, conditions):
                rows.append(_merge_rows(left_row, right_row, right_table=right_table, right_join_columns=right_join_columns))
                if limit and len(rows) >= limit:
                    break
        if limit and len(rows) >= limit:
            break
    columns = _columns_from_rows(rows) if rows else list(left.columns) + _right_output_columns(right.columns, left.columns, right_table, right_join_columns)
    trace = {
        "operation": "explicit_join",
        "join_columns": [{"left": condition.left, "right": condition.right} for condition in conditions],
        "left_rows": len(left.rows),
        "right_rows": len(right.rows),
        "rows_returned": len(rows),
        "join_type": "explicit",
    }
    return (
        TableResult(
            columns=columns,
            rows=rows,
            provenance=left.provenance + right.provenance,
            trace=left.trace + right.trace,
            truncated=left.truncated or right.truncated,
            warnings=left.warnings + right.warnings,
        ),
        trace,
    )


def _explicit_joinable(left: dict[str, Any], right: dict[str, Any], conditions: tuple[JoinCondition, ...]) -> bool:
    for condition in conditions:
        left_found, left_value = _lookup_join_value(left, condition.left)
        right_found, right_value = _lookup_join_value(right, condition.right)
        if not left_found or not right_found or str(left_value) != str(right_value):
            return False
    return True


def _merge_rows(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    right_table: str,
    right_join_columns: set[str],
) -> dict[str, Any]:
    merged = dict(left)
    right_join_suffixes = {_last_segment(column).lower() for column in right_join_columns}
    prefix = _last_segment(right_table)
    for column, value in right.items():
        if column not in merged:
            merged[column] = value
            continue
        if column.lower() in right_join_suffixes:
            continue
        merged[f"{prefix}.{column}"] = value
    return merged


def _right_output_columns(right_columns: list[str], left_columns: list[str], right_table: str, right_join_columns: set[str]) -> list[str]:
    output: list[str] = []
    left_set = set(left_columns)
    right_join_suffixes = {_last_segment(column).lower() for column in right_join_columns}
    prefix = _last_segment(right_table)
    for column in right_columns:
        if column not in left_set:
            output.append(column)
        elif column.lower() not in right_join_suffixes:
            output.append(f"{prefix}.{column}")
    return output


def _lookup_join_value(row: dict[str, Any], column: str) -> tuple[bool, Any]:
    if column in row:
        return True, row[column]
    lowered = column.lower()
    last = _last_segment(column).lower()
    for key, value in row.items():
        key_lower = key.lower()
        if key_lower == lowered or key_lower.endswith(f".{last}") or key_lower == last:
            return True, value
    return False, None


def _last_segment(value: str) -> str:
    return value.split(".")[-1]


def _columns_from_rows(rows: list[dict[str, Any]]) -> list[str]:
    columns: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for column in row:
            if column not in seen:
                seen.add(column)
                columns.append(column)
    return columns


def _source_payload(source: SourceConfig) -> dict[str, Any]:
    return {
        "name": source.name,
        "kind": source.kind,
        "path": source.path,
        "options": source.options or {},
        "enabled": source.enabled,
    }


def _source_status(source: SourceConfig) -> dict[str, Any]:
    payload = _source_payload(source)
    try:
        wrapper = make_wrapper(source)
        tables = wrapper.list_tables()
        payload.update({"ok": True, "tables": tables, "table_count": len(tables), "errors": []})
    except Exception as exc:
        payload.update({"ok": False, "tables": [], "table_count": 0, "errors": [f"{type(exc).__name__}: {exc}"]})
    return payload


def _safe_tables(source: SourceConfig) -> list[str]:
    try:
        return make_wrapper(source).list_tables()
    except Exception:
        return []
