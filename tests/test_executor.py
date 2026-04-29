from __future__ import annotations

import json
from pathlib import Path

from aql_rubicon.executor import AQLRuntime
from aql_rubicon.state import WorkspaceState


def runtime(tmp_path: Path, monkeypatch, workspace: str = "test") -> AQLRuntime:
    monkeypatch.setenv("AQL_RUBICON_HOME", str(tmp_path))
    return AQLRuntime(WorkspaceState(workspace))


def test_schema_lists_default_demo_sources(tmp_path: Path, monkeypatch) -> None:
    rt = runtime(tmp_path, monkeypatch)
    payload = rt.schema()
    assert payload["ok"] is True
    assert {source["name"] for source in payload["sources"]} >= {"demo_university", "demo_lab", "wikipedia"}


def test_find_query_against_demo_sqlite(tmp_path: Path, monkeypatch) -> None:
    payload = runtime(tmp_path, monkeypatch).execute_script(
        'FIND person_id, full_name FROM demo_university.faculty WHERE department = "Computer Science"'
    )
    assert payload["ok"] is True
    assert payload["row_count"] == 4
    assert payload["columns"] == ["person_id", "full_name"]


def test_find_query_reports_truncation(tmp_path: Path, monkeypatch) -> None:
    payload = runtime(tmp_path, monkeypatch).execute_script(
        'FIND person_id, full_name FROM demo_university.faculty WHERE department = "Computer Science"',
        limit=2,
    )
    assert payload["ok"] is True
    assert payload["row_count"] == 2
    assert payload["truncated"] is True


def test_aggregate_count(tmp_path: Path, monkeypatch) -> None:
    payload = runtime(tmp_path, monkeypatch).execute_script(
        'FIND count(*) FROM demo_university.faculty WHERE department = "Computer Science"'
    )
    assert payload["ok"] is True
    assert payload["rows"] == [{"count(*)": 4}]


def test_join_between_demo_sources(tmp_path: Path, monkeypatch) -> None:
    payload = runtime(tmp_path, monkeypatch).execute_script(
        'FIND person_id, full_name FROM demo_university.faculty WHERE title = "Professor" '
        'JOIN FIND person_id, lab_role FROM demo_lab.people WHERE lab_role = "Research Lab Professor"'
    )
    assert payload["ok"] is True
    assert payload["row_count"] == 3
    assert {row["person_id"] for row in payload["rows"]} == {"p001", "p002", "p005"}
    assert any(step.get("operation") == "natural_join" for step in payload["trace"])


def test_save_output_delete_table(tmp_path: Path, monkeypatch) -> None:
    rt = runtime(tmp_path, monkeypatch)
    save = rt.execute_script(
        'SAVE (FIND person_id, lab_role FROM demo_lab.people WHERE lab_role = "Research Lab Professor") AS lab_professors'
    )
    assert save["ok"] is True
    assert save["saved_table"] == "lab_professors"
    output = rt.execute_script("OUTPUT lab_professors")
    assert output["ok"] is True
    assert output["row_count"] == 3
    delete = rt.execute_script("DELETE lab_professors")
    assert delete["ok"] is True
    assert delete["deleted"] is True


def test_register_csv_source(tmp_path: Path, monkeypatch) -> None:
    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()
    (csv_dir / "tickets.csv").write_text("id,status,priority\n1,open,high\n2,closed,low\n", encoding="utf-8")
    rt = runtime(tmp_path, monkeypatch)
    registered = rt.register_source("tickets", "csv_dir", path=str(csv_dir), options={})
    assert registered["ok"] is True
    payload = rt.execute_script('FIND id, priority FROM tickets.tickets WHERE status = "open"')
    assert payload["rows"] == [{"id": "1", "priority": "high"}]


def test_tool_handler_returns_json(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AQL_RUBICON_HOME", str(tmp_path))
    from tools import aql_schema

    payload = json.loads(aql_schema({"workspace": "tooltest"}))
    assert payload["ok"] is True
    assert "sources" in payload
