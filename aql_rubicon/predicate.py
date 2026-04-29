"""Deterministic natural-language predicate adapters."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Iterable


_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "has",
    "have",
    "in",
    "is",
    "me",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}

_CONTAINS_RE = re.compile(
    r"^(?P<column>[A-Za-z_][A-Za-z0-9_.-]*)\s+(?:contains|including|includes|mentions|like)\s+(?P<value>.+)$",
    re.I,
)
_COMPARE_RE = re.compile(
    r"^(?P<column>[A-Za-z_][A-Za-z0-9_.-]*)\s*(?P<op>>=|<=|!=|=|>|<|==)\s*(?P<value>.+)$",
    re.I,
)
_WORD_COMPARE_RE = re.compile(
    r"^(?P<column>[A-Za-z_][A-Za-z0-9_.-]*)\s+(?P<op>greater than or equal to|less than or equal to|greater than|less than|after|before|on or after|on or before|equals|is|not)\s+(?P<value>.+)$",
    re.I,
)


def filter_rows(rows: Iterable[dict[str, Any]], predicate: str | None) -> list[dict[str, Any]]:
    """Filter rows using deterministic rule-based predicate translation."""
    if not predicate or predicate.strip() in {"*", "all", "ALL"}:
        return list(rows)
    clauses = _split_clauses(predicate)
    result: list[dict[str, Any]] = []
    for row in rows:
        if all(matches(row, clause) for clause in clauses):
            result.append(row)
    return result


def matches(row: dict[str, Any], predicate: str) -> bool:
    text = _strip_quotes(predicate.strip())
    if not text:
        return True
    for parser in (_contains_match, _symbol_compare_match, _word_compare_match):
        parsed = parser(text)
        if parsed is not None:
            column, op, value = parsed
            return _evaluate(row, column, op, value)
    return _keyword_match(row, text)


def extract_keywords(predicate: str | None) -> list[str]:
    if not predicate:
        return []
    tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]*", predicate.lower())
    return [token for token in tokens if token not in _STOPWORDS]


def _split_clauses(predicate: str) -> list[str]:
    parts = re.split(r"\s+\bAND\b\s+", predicate, flags=re.I)
    return [part.strip() for part in parts if part.strip()]


def _contains_match(text: str) -> tuple[str, str, str] | None:
    match = _CONTAINS_RE.match(text)
    if not match:
        return None
    return match.group("column"), "contains", _strip_quotes(match.group("value"))


def _symbol_compare_match(text: str) -> tuple[str, str, str] | None:
    match = _COMPARE_RE.match(text)
    if not match:
        return None
    return match.group("column"), match.group("op"), _strip_quotes(match.group("value"))


def _word_compare_match(text: str) -> tuple[str, str, str] | None:
    match = _WORD_COMPARE_RE.match(text)
    if not match:
        return None
    op = match.group("op").lower()
    mapping = {
        "greater than": ">",
        "less than": "<",
        "greater than or equal to": ">=",
        "less than or equal to": "<=",
        "after": ">",
        "before": "<",
        "on or after": ">=",
        "on or before": "<=",
        "equals": "=",
        "is": "=",
        "not": "!=",
    }
    return match.group("column"), mapping[op], _strip_quotes(match.group("value"))


def _evaluate(row: dict[str, Any], column: str, op: str, value: str) -> bool:
    if column not in row:
        lowered = column.lower()
        matches = [key for key in row if key.lower() == lowered or key.lower().endswith(f".{lowered}")]
        if not matches:
            return False
        column = matches[0]
    cell = row.get(column)
    if op == "contains":
        return value.lower() in str(cell or "").lower()
    left, right = _coerce_pair(cell, value)
    if op in {"=", "=="}:
        return left == right
    if op == "!=":
        return left != right
    if op == ">":
        return left > right
    if op == "<":
        return left < right
    if op == ">=":
        return left >= right
    if op == "<=":
        return left <= right
    return False


def _keyword_match(row: dict[str, Any], predicate: str) -> bool:
    keywords = extract_keywords(predicate)
    if not keywords:
        return True
    haystack = " ".join(str(value).lower() for value in row.values() if value is not None)
    return all(keyword in haystack for keyword in keywords)


def _coerce_pair(left: Any, right: str) -> tuple[Any, Any]:
    left_num = _to_number(left)
    right_num = _to_number(right)
    if left_num is not None and right_num is not None:
        return left_num, right_num
    left_date = _to_datetime(left)
    right_date = _to_datetime(right)
    if left_date is not None and right_date is not None:
        return left_date, right_date
    return str(left or "").lower(), str(right or "").lower()


def _to_number(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def _to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y", "%m/%d/%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _strip_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value

