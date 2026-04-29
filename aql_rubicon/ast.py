"""AQL abstract syntax tree models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class SelectItem:
    """A selected column or aggregate expression."""

    raw: str
    column: str
    aggregate: str | None = None
    alias: str | None = None

    @property
    def output_name(self) -> str:
        if self.alias:
            return self.alias
        if self.aggregate:
            return f"{self.aggregate}({self.column})"
        return self.column


@dataclass(frozen=True)
class FindQuery:
    columns: tuple[SelectItem, ...]
    table: str
    where: str | None = None


@dataclass(frozen=True)
class JoinCondition:
    left: str
    right: str


@dataclass(frozen=True)
class JoinStep:
    query: FindQuery
    conditions: tuple[JoinCondition, ...] = ()


@dataclass(frozen=True)
class JoinQuery:
    first: FindQuery
    steps: tuple[JoinStep, ...]

    @property
    def parts(self) -> tuple[FindQuery, ...]:
        return (self.first,) + tuple(step.query for step in self.steps)


@dataclass(frozen=True)
class SaveCommand:
    query: FindQuery | JoinQuery
    table: str


@dataclass(frozen=True)
class OutputCommand:
    table: str


@dataclass(frozen=True)
class DeleteCommand:
    table: str


@dataclass(frozen=True)
class SchemaCommand:
    target: str | None = None


@dataclass(frozen=True)
class ExplainCommand:
    command: FindQuery | JoinQuery | SaveCommand | OutputCommand | DeleteCommand | SchemaCommand


Command = FindQuery | JoinQuery | SaveCommand | OutputCommand | DeleteCommand | SchemaCommand | ExplainCommand


@dataclass(frozen=True)
class SourceConfig:
    name: str
    kind: Literal["sqlite", "csv_dir", "json_dir", "wikipedia"]
    path: str | None = None
    options: dict | None = None
    enabled: bool = True
