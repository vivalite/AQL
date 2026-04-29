from __future__ import annotations

from aql_rubicon.predicate import KeywordNode, compile_sql, parse_predicate


def test_predicate_and_or_parentheses() -> None:
    node = parse_predicate('(department = "Computer Science" OR department = Mathematics) AND title contains Professor')
    assert node.evaluate({"department": "Computer Science", "title": "Assistant Professor"})
    assert node.evaluate({"department": "Mathematics", "title": "Professor"})
    assert not node.evaluate({"department": "Physics", "title": "Professor"})


def test_predicate_in_between_and_nulls() -> None:
    in_node = parse_predicate('department IN ("Computer Science", Mathematics)')
    assert in_node.evaluate({"department": "Mathematics"})
    between = parse_predicate("opened_year BETWEEN 1980 AND 2000")
    assert between.evaluate({"opened_year": 1996})
    assert not between.evaluate({"opened_year": 2005})
    assert parse_predicate("closed_at IS NULL").evaluate({"closed_at": None})
    assert parse_predicate("closed_at IS NOT NULL").evaluate({"closed_at": "2026-01-01"})


def test_plain_text_falls_back_to_keyword_node() -> None:
    node = parse_predicate("programming languages")
    assert isinstance(node, KeywordNode)
    assert node.evaluate({"summary": "Research on programming languages and compilers"})


def test_compile_sql_for_supported_predicate() -> None:
    compiled = compile_sql('department = "Computer Science" AND opened_year >= 2000', ["department", "opened_year"])
    assert compiled.supported is True
    assert '"department" = ?' in compiled.sql
    assert '"opened_year" >= ?' in compiled.sql
    assert compiled.params == ("Computer Science", 2000)

