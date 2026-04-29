"""Parser for the restricted AQL notation described in the RUBICON paper."""

from __future__ import annotations

import re
from typing import Iterable

from .ast import (
    Command,
    DeleteCommand,
    FindQuery,
    JoinQuery,
    OutputCommand,
    SaveCommand,
    SchemaCommand,
    SelectItem,
)


class AQLSyntaxError(ValueError):
    """Raised when a command does not match the supported AQL subset."""


_IDENT = r"[A-Za-z_][A-Za-z0-9_.-]*"
_SAVE_RE = re.compile(rf"^SAVE\s*\((?P<query>.*)\)\s+AS\s+(?P<table>{_IDENT})$", re.I | re.S)
_FIND_RE = re.compile(
    rf"^FIND\s+(?P<columns>.*?)\s+FROM\s+(?P<table>{_IDENT})(?:\s+WHERE\s+(?P<where>.*))?$",
    re.I | re.S,
)
_AGG_RE = re.compile(
    r"^(?P<name>count|sum|avg|min|max)\s*\(\s*(?P<column>\*|[A-Za-z_][A-Za-z0-9_.-]*)\s*\)(?:\s+AS\s+(?P<alias>[A-Za-z_][A-Za-z0-9_]*))?$",
    re.I,
)
_ALIAS_RE = re.compile(r"^(?P<column>[A-Za-z_][A-Za-z0-9_.-]*|\*)\s+AS\s+(?P<alias>[A-Za-z_][A-Za-z0-9_]*)$", re.I)


def split_script(script: str) -> list[str]:
    """Split semicolon-separated AQL while respecting parentheses and quotes."""
    commands: list[str] = []
    buf: list[str] = []
    depth = 0
    quote: str | None = None
    escape = False
    for char in script:
        if escape:
            buf.append(char)
            escape = False
            continue
        if char == "\\" and quote:
            buf.append(char)
            escape = True
            continue
        if quote:
            buf.append(char)
            if char == quote:
                quote = None
            continue
        if char in {"'", '"'}:
            quote = char
            buf.append(char)
            continue
        if char == "(":
            depth += 1
            buf.append(char)
            continue
        if char == ")":
            depth = max(0, depth - 1)
            buf.append(char)
            continue
        if char == ";" and depth == 0:
            command = "".join(buf).strip()
            if command:
                commands.append(command)
            buf = []
            continue
        buf.append(char)
    tail = "".join(buf).strip()
    if tail:
        commands.append(tail)
    return commands


def parse(command: str) -> Command:
    text = _normalize(command)
    if not text:
        raise AQLSyntaxError("empty AQL command")
    if text.startswith("?"):
        target = text[1:].strip()
        return SchemaCommand(target or None)
    upper = text.upper()
    if upper.startswith("SAVE"):
        return _parse_save(text)
    if upper.startswith("OUTPUT"):
        parts = text.split(None, 1)
        if len(parts) != 2 or not parts[1].strip():
            raise AQLSyntaxError("OUTPUT requires a table name")
        return OutputCommand(parts[1].strip())
    if upper.startswith("DELETE"):
        parts = text.split(None, 1)
        if len(parts) != 2 or not parts[1].strip():
            raise AQLSyntaxError("DELETE requires a table name")
        return DeleteCommand(parts[1].strip())
    return _parse_query(text)


def _parse_save(text: str) -> SaveCommand:
    match = _SAVE_RE.match(text)
    if not match:
        raise AQLSyntaxError("SAVE syntax is SAVE (<query>) AS <table>")
    return SaveCommand(query=_parse_query(match.group("query")), table=match.group("table"))


def _parse_query(text: str) -> FindQuery | JoinQuery:
    parts = _split_join(text)
    queries = tuple(_parse_find(part) for part in parts)
    if len(queries) == 1:
        return queries[0]
    return JoinQuery(queries)


def _parse_find(text: str) -> FindQuery:
    match = _FIND_RE.match(text.strip())
    if not match:
        raise AQLSyntaxError("query syntax is FIND <columns> FROM <table> [WHERE <predicate>]")
    return FindQuery(
        columns=tuple(_parse_select_items(_split_csv(match.group("columns")))),
        table=match.group("table").strip(),
        where=(match.group("where") or "").strip() or None,
    )


def _parse_select_items(items: Iterable[str]) -> list[SelectItem]:
    parsed: list[SelectItem] = []
    for item in items:
        raw = item.strip()
        if not raw:
            continue
        agg = _AGG_RE.match(raw)
        if agg:
            parsed.append(
                SelectItem(
                    raw=raw,
                    column=agg.group("column"),
                    aggregate=agg.group("name").lower(),
                    alias=agg.group("alias"),
                )
            )
            continue
        alias = _ALIAS_RE.match(raw)
        if alias:
            parsed.append(SelectItem(raw=raw, column=alias.group("column"), alias=alias.group("alias")))
            continue
        if not re.match(r"^([A-Za-z_][A-Za-z0-9_.-]*|\*)$", raw):
            raise AQLSyntaxError(f"invalid selected column: {raw}")
        parsed.append(SelectItem(raw=raw, column=raw))
    if not parsed:
        raise AQLSyntaxError("FIND requires at least one column")
    return parsed


def _split_join(text: str) -> list[str]:
    tokens: list[str] = []
    start = 0
    depth = 0
    quote: str | None = None
    i = 0
    while i < len(text):
        char = text[i]
        if quote:
            if char == quote:
                quote = None
            i += 1
            continue
        if char in {"'", '"'}:
            quote = char
            i += 1
            continue
        if char == "(":
            depth += 1
            i += 1
            continue
        if char == ")":
            depth = max(0, depth - 1)
            i += 1
            continue
        if depth == 0 and text[i : i + 4].upper() == "JOIN":
            before = text[i - 1] if i > 0 else " "
            after = text[i + 4] if i + 4 < len(text) else " "
            if before.isspace() and after.isspace():
                tokens.append(text[start:i].strip())
                start = i + 4
                i += 4
                continue
        i += 1
    tokens.append(text[start:].strip())
    if any(not token for token in tokens):
        raise AQLSyntaxError("JOIN requires a FIND query on both sides")
    return tokens


def _split_csv(text: str) -> list[str]:
    items: list[str] = []
    buf: list[str] = []
    depth = 0
    for char in text:
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        if char == "," and depth == 0:
            items.append("".join(buf).strip())
            buf = []
            continue
        buf.append(char)
    items.append("".join(buf).strip())
    return items


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())
