"""Deterministic predicate parsing, evaluation, and SQLite compilation."""

from __future__ import annotations

import re
from dataclasses import dataclass
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

_TOKEN_RE = re.compile(
    r"""
    (?P<SPACE>\s+)
  | (?P<STRING>"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')
  | (?P<OP>>=|<=|!=|==|=|>|<)
  | (?P<LPAREN>\()
  | (?P<RPAREN>\))
  | (?P<COMMA>,)
  | (?P<NUMBER>-?\d+(?:\.\d+)?)
  | (?P<IDENT>[A-Za-z_][A-Za-z0-9_.-]*)
  | (?P<OTHER>\S)
    """,
    re.VERBOSE,
)


class PredicateSyntaxError(ValueError):
    """Raised when a structured predicate is malformed."""


@dataclass(frozen=True)
class Token:
    kind: str
    value: Any


class PredicateNode:
    def evaluate(self, row: dict[str, Any]) -> bool:
        raise NotImplementedError

    def to_plan(self) -> dict[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True)
class AllNode(PredicateNode):
    def evaluate(self, row: dict[str, Any]) -> bool:
        return True

    def to_plan(self) -> dict[str, Any]:
        return {"type": "all"}


@dataclass(frozen=True)
class KeywordNode(PredicateNode):
    text: str

    def evaluate(self, row: dict[str, Any]) -> bool:
        keywords = extract_keywords(self.text)
        if not keywords:
            return True
        haystack = " ".join(str(value).lower() for value in row.values() if value is not None)
        return all(keyword in haystack for keyword in keywords)

    def to_plan(self) -> dict[str, Any]:
        return {"type": "keyword", "keywords": extract_keywords(self.text), "text": self.text}


@dataclass(frozen=True)
class BoolNode(PredicateNode):
    op: str
    left: PredicateNode
    right: PredicateNode

    def evaluate(self, row: dict[str, Any]) -> bool:
        if self.op == "AND":
            return self.left.evaluate(row) and self.right.evaluate(row)
        return self.left.evaluate(row) or self.right.evaluate(row)

    def to_plan(self) -> dict[str, Any]:
        return {"type": self.op.lower(), "left": self.left.to_plan(), "right": self.right.to_plan()}


@dataclass(frozen=True)
class ComparisonNode(PredicateNode):
    column: str
    op: str
    value: Any

    def evaluate(self, row: dict[str, Any]) -> bool:
        found, cell = _lookup(row, self.column)
        if not found:
            return False
        left, right = _coerce_pair(cell, self.value)
        if self.op in {"=", "=="}:
            return left == right
        if self.op == "!=":
            return left != right
        if self.op == ">":
            return left > right
        if self.op == "<":
            return left < right
        if self.op == ">=":
            return left >= right
        if self.op == "<=":
            return left <= right
        return False

    def to_plan(self) -> dict[str, Any]:
        return {"type": "comparison", "column": self.column, "op": self.op, "value": self.value}


@dataclass(frozen=True)
class ContainsNode(PredicateNode):
    column: str
    value: str

    def evaluate(self, row: dict[str, Any]) -> bool:
        found, cell = _lookup(row, self.column)
        if not found:
            return False
        return self.value.lower() in str(cell or "").lower()

    def to_plan(self) -> dict[str, Any]:
        return {"type": "contains", "column": self.column, "value": self.value}


@dataclass(frozen=True)
class InNode(PredicateNode):
    column: str
    values: tuple[Any, ...]

    def evaluate(self, row: dict[str, Any]) -> bool:
        found, cell = _lookup(row, self.column)
        if not found:
            return False
        return any(_coerce_pair(cell, value)[0] == _coerce_pair(cell, value)[1] for value in self.values)

    def to_plan(self) -> dict[str, Any]:
        return {"type": "in", "column": self.column, "values": list(self.values)}


@dataclass(frozen=True)
class BetweenNode(PredicateNode):
    column: str
    low: Any
    high: Any

    def evaluate(self, row: dict[str, Any]) -> bool:
        found, cell = _lookup(row, self.column)
        if not found:
            return False
        value, low = _coerce_pair(cell, self.low)
        value_again, high = _coerce_pair(cell, self.high)
        return value >= low and value_again <= high

    def to_plan(self) -> dict[str, Any]:
        return {"type": "between", "column": self.column, "low": self.low, "high": self.high}


@dataclass(frozen=True)
class NullNode(PredicateNode):
    column: str
    negate: bool = False

    def evaluate(self, row: dict[str, Any]) -> bool:
        found, cell = _lookup(row, self.column)
        is_null = not found or cell in (None, "")
        return not is_null if self.negate else is_null

    def to_plan(self) -> dict[str, Any]:
        return {"type": "null_check", "column": self.column, "negate": self.negate}


@dataclass(frozen=True)
class SqlPredicate:
    supported: bool
    sql: str = ""
    params: tuple[Any, ...] = ()
    warnings: tuple[str, ...] = ()


def parse_predicate(predicate: str | None) -> PredicateNode:
    """Parse a deterministic predicate. Unknown prose becomes keyword search."""
    if not predicate or predicate.strip() in {"*", "all", "ALL"}:
        return AllNode()
    text = predicate.strip()
    try:
        parser = _Parser(_tokenize(text))
        node = parser.parse()
        if parser.at_end():
            return node
    except PredicateSyntaxError:
        pass
    return KeywordNode(text)


def filter_rows(rows: Iterable[dict[str, Any]], predicate: str | None) -> list[dict[str, Any]]:
    """Filter rows using the shared deterministic predicate evaluator."""
    node = parse_predicate(predicate)
    return [row for row in rows if node.evaluate(row)]


def matches(row: dict[str, Any], predicate: str) -> bool:
    return parse_predicate(predicate).evaluate(row)


def predicate_plan(predicate: str | None) -> dict[str, Any]:
    return parse_predicate(predicate).to_plan()


def compile_sql(predicate: str | None, columns: Iterable[str]) -> SqlPredicate:
    node = parse_predicate(predicate)
    column_map = {column.lower(): column for column in columns}
    return _compile_node(node, column_map)


def extract_keywords(predicate: str | None) -> list[str]:
    if not predicate:
        return []
    tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]*", predicate.lower())
    return [token for token in tokens if token not in _STOPWORDS]


class _Parser:
    def __init__(self, tokens: list[Token]) -> None:
        self.tokens = tokens
        self.pos = 0

    def parse(self) -> PredicateNode:
        return self._or()

    def at_end(self) -> bool:
        return self._peek().kind == "EOF"

    def _or(self) -> PredicateNode:
        node = self._and()
        while self._match_keyword("OR"):
            node = BoolNode("OR", node, self._and())
        return node

    def _and(self) -> PredicateNode:
        node = self._primary()
        while self._match_keyword("AND"):
            node = BoolNode("AND", node, self._primary())
        return node

    def _primary(self) -> PredicateNode:
        if self._match("LPAREN"):
            node = self._or()
            self._expect("RPAREN")
            return node
        return self._comparison()

    def _comparison(self) -> PredicateNode:
        column = self._expect("IDENT").value
        if self._match_keyword("CONTAINS", "INCLUDING", "INCLUDES", "MENTIONS", "LIKE"):
            return ContainsNode(column, str(self._value()))
        if self._match_keyword("IN"):
            self._expect("LPAREN")
            values = [self._value()]
            while self._match("COMMA"):
                values.append(self._value())
            self._expect("RPAREN")
            return InNode(column, tuple(values))
        if self._match_keyword("BETWEEN"):
            low = self._value()
            self._expect_keyword("AND")
            return BetweenNode(column, low, self._value())
        if self._match_keyword("IS"):
            negate = self._match_keyword("NOT")
            self._expect_keyword("NULL")
            return NullNode(column, negate=negate)
        word_op = self._word_operator()
        if word_op:
            return ComparisonNode(column, word_op, self._value())
        if self._peek().kind == "OP":
            op = self._advance().value
            return ComparisonNode(column, op, self._value())
        raise PredicateSyntaxError("expected predicate operator")

    def _word_operator(self) -> str | None:
        if self._match_keyword("AFTER"):
            return ">"
        if self._match_keyword("BEFORE"):
            return "<"
        if self._match_keyword("EQUALS"):
            return "="
        if self._match_keyword("NOT"):
            return "!="
        if self._match_keyword("IS"):
            return "="
        if self._match_keyword("GREATER"):
            self._expect_keyword("THAN")
            if self._match_keyword("OR"):
                self._expect_keyword("EQUAL")
                self._expect_keyword("TO")
                return ">="
            return ">"
        if self._match_keyword("LESS"):
            self._expect_keyword("THAN")
            if self._match_keyword("OR"):
                self._expect_keyword("EQUAL")
                self._expect_keyword("TO")
                return "<="
            return "<"
        if self._match_keyword("ON"):
            if self._match_keyword("OR"):
                if self._match_keyword("AFTER"):
                    return ">="
                if self._match_keyword("BEFORE"):
                    return "<="
                raise PredicateSyntaxError("expected AFTER or BEFORE")
            return "="
        return None

    def _value(self) -> Any:
        token = self._advance()
        if token.kind in {"STRING", "NUMBER", "IDENT"}:
            return token.value
        raise PredicateSyntaxError("expected value")

    def _match(self, kind: str) -> bool:
        if self._peek().kind == kind:
            self._advance()
            return True
        return False

    def _match_keyword(self, *values: str) -> bool:
        if self._peek().kind == "IDENT" and str(self._peek().value).upper() in values:
            self._advance()
            return True
        return False

    def _expect(self, kind: str) -> Token:
        token = self._advance()
        if token.kind != kind:
            raise PredicateSyntaxError(f"expected {kind}")
        return token

    def _expect_keyword(self, value: str) -> None:
        if not self._match_keyword(value):
            raise PredicateSyntaxError(f"expected {value}")

    def _peek(self) -> Token:
        return self.tokens[self.pos]

    def _advance(self) -> Token:
        token = self.tokens[self.pos]
        self.pos += 1
        return token


def _tokenize(text: str) -> list[Token]:
    tokens: list[Token] = []
    for match in _TOKEN_RE.finditer(text):
        kind = match.lastgroup or "OTHER"
        raw = match.group(kind)
        if kind == "SPACE":
            continue
        if kind == "STRING":
            tokens.append(Token(kind, _unquote(raw)))
        elif kind == "NUMBER":
            tokens.append(Token(kind, float(raw) if "." in raw else int(raw)))
        elif kind == "OTHER":
            raise PredicateSyntaxError(f"unexpected token: {raw}")
        else:
            tokens.append(Token(kind, raw))
    tokens.append(Token("EOF", ""))
    return tokens


def _compile_node(node: PredicateNode, columns: dict[str, str]) -> SqlPredicate:
    if isinstance(node, AllNode):
        return SqlPredicate(True, "1 = 1")
    if isinstance(node, KeywordNode):
        keywords = extract_keywords(node.text)
        if not keywords:
            return SqlPredicate(True, "1 = 1")
        if not columns:
            return SqlPredicate(False, warnings=("keyword predicate needs at least one column",))
        clauses: list[str] = []
        params: list[Any] = []
        searchable = list(columns.values())
        for keyword in keywords:
            per_keyword = [f"CAST({_quote_identifier(column)} AS TEXT) LIKE ?" for column in searchable]
            clauses.append("(" + " OR ".join(per_keyword) + ")")
            params.extend([f"%{keyword}%"] * len(searchable))
        return SqlPredicate(True, " AND ".join(clauses), tuple(params))
    if isinstance(node, BoolNode):
        left = _compile_node(node.left, columns)
        right = _compile_node(node.right, columns)
        if not left.supported or not right.supported:
            return SqlPredicate(False, warnings=left.warnings + right.warnings)
        return SqlPredicate(True, f"({left.sql}) {node.op} ({right.sql})", left.params + right.params)
    if isinstance(node, ComparisonNode):
        column = _resolve_column(node.column, columns)
        if not column:
            return SqlPredicate(False, warnings=(f"unknown column in predicate: {node.column}",))
        return SqlPredicate(True, f"{_quote_identifier(column)} {node.op} ?", (_sql_value(node.value),))
    if isinstance(node, ContainsNode):
        column = _resolve_column(node.column, columns)
        if not column:
            return SqlPredicate(False, warnings=(f"unknown column in predicate: {node.column}",))
        return SqlPredicate(True, f"CAST({_quote_identifier(column)} AS TEXT) LIKE ?", (f"%{node.value}%",))
    if isinstance(node, InNode):
        column = _resolve_column(node.column, columns)
        if not column:
            return SqlPredicate(False, warnings=(f"unknown column in predicate: {node.column}",))
        if not node.values:
            return SqlPredicate(True, "0 = 1")
        placeholders = ", ".join("?" for _ in node.values)
        return SqlPredicate(True, f"{_quote_identifier(column)} IN ({placeholders})", tuple(_sql_value(v) for v in node.values))
    if isinstance(node, BetweenNode):
        column = _resolve_column(node.column, columns)
        if not column:
            return SqlPredicate(False, warnings=(f"unknown column in predicate: {node.column}",))
        return SqlPredicate(True, f"{_quote_identifier(column)} BETWEEN ? AND ?", (_sql_value(node.low), _sql_value(node.high)))
    if isinstance(node, NullNode):
        column = _resolve_column(node.column, columns)
        if not column:
            return SqlPredicate(False, warnings=(f"unknown column in predicate: {node.column}",))
        op = "IS NOT NULL" if node.negate else "IS NULL"
        return SqlPredicate(True, f"{_quote_identifier(column)} {op}")
    return SqlPredicate(False, warnings=(f"unsupported predicate node: {type(node).__name__}",))


def _lookup(row: dict[str, Any], column: str) -> tuple[bool, Any]:
    if column in row:
        return True, row[column]
    lowered = column.lower()
    for key, value in row.items():
        if key.lower() == lowered or key.lower().endswith(f".{lowered}"):
            return True, value
    return False, None


def _resolve_column(column: str, columns: dict[str, str]) -> str | None:
    if column.lower() in columns:
        return columns[column.lower()]
    suffix = f".{column.lower()}"
    matches = [real for lowered, real in columns.items() if lowered.endswith(suffix)]
    return matches[0] if len(matches) == 1 else None


def _coerce_pair(left: Any, right: Any) -> tuple[Any, Any]:
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


def _sql_value(value: Any) -> Any:
    if isinstance(value, (int, float)):
        return value
    return str(value)


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1].replace(f"\\{value[0]}", value[0]).replace("\\\\", "\\")
    return value


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'

