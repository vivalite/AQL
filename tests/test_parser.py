from __future__ import annotations

import pytest

from aql_rubicon.ast import ExplainCommand, FindQuery, JoinQuery, SaveCommand, SchemaCommand
from aql_rubicon.parser import AQLSyntaxError, parse, split_script


def test_parse_find_with_where() -> None:
    command = parse('FIND person_id, full_name FROM demo_university.faculty WHERE department = "Computer Science"')
    assert isinstance(command, FindQuery)
    assert command.table == "demo_university.faculty"
    assert [item.column for item in command.columns] == ["person_id", "full_name"]
    assert command.where == 'department = "Computer Science"'


def test_parse_join() -> None:
    command = parse(
        "FIND person_id FROM faculty WHERE title = Professor "
        "JOIN FIND person_id FROM people WHERE lab_role contains Professor"
    )
    assert isinstance(command, JoinQuery)
    assert len(command.parts) == 2
    assert command.steps[0].conditions == ()


def test_parse_explicit_join_conditions() -> None:
    command = parse(
        "FIND person_id FROM faculty "
        "JOIN FIND researcher_id FROM lab_people ON person_id = researcher_id"
    )
    assert isinstance(command, JoinQuery)
    assert command.steps[0].conditions[0].left == "person_id"
    assert command.steps[0].conditions[0].right == "researcher_id"


def test_parse_explain() -> None:
    command = parse("EXPLAIN FIND person_id FROM faculty WHERE title IN (Professor, Lecturer)")
    assert isinstance(command, ExplainCommand)
    assert isinstance(command.command, FindQuery)


def test_parse_save() -> None:
    command = parse("SAVE (FIND count(*) FROM faculty WHERE title = Professor) AS professor_count")
    assert isinstance(command, SaveCommand)
    assert command.table == "professor_count"
    assert isinstance(command.query, FindQuery)
    assert command.query.columns[0].aggregate == "count"


def test_parse_schema_command() -> None:
    command = parse("? demo_university")
    assert isinstance(command, SchemaCommand)
    assert command.target == "demo_university"


def test_split_script_respects_save_parentheses() -> None:
    script = "SAVE (FIND person_id FROM faculty WHERE title = Professor) AS x; OUTPUT x"
    assert split_script(script) == [
        "SAVE (FIND person_id FROM faculty WHERE title = Professor) AS x",
        "OUTPUT x",
    ]


def test_invalid_find_raises() -> None:
    with pytest.raises(AQLSyntaxError):
        parse("FIND FROM faculty")
