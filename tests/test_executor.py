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
    assert any(step.get("pushdown") is True for step in payload["trace"])


def test_aggregate_count(tmp_path: Path, monkeypatch) -> None:
    payload = runtime(tmp_path, monkeypatch).execute_script(
        'FIND count(*) FROM demo_university.faculty WHERE department = "Computer Science"'
    )
    assert payload["ok"] is True
    assert payload["rows"] == [{"count(*)": 4}]
    assert any(step.get("aggregate_pushdown") is True for step in payload["trace"])


def test_join_between_demo_sources(tmp_path: Path, monkeypatch) -> None:
    payload = runtime(tmp_path, monkeypatch).execute_script(
        'FIND person_id, full_name FROM demo_university.faculty WHERE title = "Professor" '
        'JOIN FIND person_id, lab_role FROM demo_lab.people WHERE lab_role = "Research Lab Professor"'
    )
    assert payload["ok"] is True
    assert payload["row_count"] == 3
    assert {row["person_id"] for row in payload["rows"]} == {"p001", "p002", "p005"}
    assert any(step.get("operation") == "natural_join" for step in payload["trace"])


def test_explicit_join_with_different_column_names_and_collision(tmp_path: Path, monkeypatch) -> None:
    aliases_dir = tmp_path / "aliases"
    aliases_dir.mkdir()
    (aliases_dir / "aliases.json").write_text(
        """
        [
          {"researcher_id": "p001", "full_name": "Lab Ada", "lab_role": "Research Lab Professor"},
          {"researcher_id": "p002", "full_name": "Lab Grace", "lab_role": "Research Lab Professor"},
          {"researcher_id": "p004", "full_name": "Lab Alan", "lab_role": "Research Lab Affiliate"}
        ]
        """,
        encoding="utf-8",
    )
    rt = runtime(tmp_path, monkeypatch)
    rt.register_source("aliases", "json_dir", path=str(aliases_dir), options={})
    payload = rt.execute_script(
        'FIND person_id, full_name FROM demo_university.faculty WHERE title = "Professor" '
        'JOIN FIND researcher_id, full_name, lab_role FROM aliases.aliases WHERE lab_role contains Professor '
        'ON person_id = researcher_id'
    )
    assert payload["ok"] is True
    assert payload["row_count"] == 2
    assert payload["columns"] == ["person_id", "full_name", "researcher_id", "aliases.full_name", "lab_role"]
    assert {row["aliases.full_name"] for row in payload["rows"]} == {"Lab Ada", "Lab Grace"}
    assert any(step.get("operation") == "explicit_join" for step in payload["trace"])


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


def test_sources_disable_enable_delete_and_status(tmp_path: Path, monkeypatch) -> None:
    rt = runtime(tmp_path, monkeypatch)
    disabled = rt.sources("disable", name="demo_lab")
    assert disabled["source"]["enabled"] is False
    listed = rt.sources("list")
    assert any(source["name"] == "demo_lab" and source["enabled"] is False for source in listed["sources"])
    failed = rt.execute_script("FIND * FROM demo_lab.people")
    assert failed["ok"] is False
    enabled = rt.sources("enable", name="demo_lab")
    assert enabled["source"]["enabled"] is True
    status = rt.sources("status", name="demo_lab")
    assert status["source"]["ok"] is True

    csv_dir = tmp_path / "csv_delete"
    csv_dir.mkdir()
    (csv_dir / "tickets.csv").write_text("id,status\n1,open\n", encoding="utf-8")
    rt.register_source("delete_me", "csv_dir", path=str(csv_dir), options={})
    deleted = rt.sources("delete", name="delete_me")
    assert deleted["deleted"] is True
    assert "delete_me" not in {source["name"] for source in rt.sources("list")["sources"]}


def test_explain_script_reports_pushdown_and_join_plan(tmp_path: Path, monkeypatch) -> None:
    payload = runtime(tmp_path, monkeypatch).explain_script(
        'FIND person_id FROM demo_university.faculty WHERE department = "Computer Science" '
        'JOIN FIND person_id FROM demo_lab.people WHERE lab_role contains Professor ON person_id = person_id'
    )
    assert payload["ok"] is True
    plan = payload["results"][0]["plan"]
    assert plan["type"] == "join"
    assert plan["first"]["pushdown"] is True
    assert plan["steps"][0]["join_type"] == "explicit"


def test_tool_handler_returns_json(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AQL_RUBICON_HOME", str(tmp_path))
    from tools import aql_explain, aql_schema, aql_sources

    payload = json.loads(aql_schema({"workspace": "tooltest"}))
    assert payload["ok"] is True
    assert "sources" in payload
    sources = json.loads(aql_sources({"workspace": "tooltest", "action": "list"}))
    assert sources["ok"] is True
    explained = json.loads(aql_explain({"workspace": "tooltest", "query": "FIND * FROM demo_lab.people"}))
    assert explained["ok"] is True
