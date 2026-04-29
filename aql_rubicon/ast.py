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
class JoinQuery:
    parts: tuple[FindQuery, ...]


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


Command = FindQuery | JoinQuery | SaveCommand | OutputCommand | DeleteCommand | SchemaCommand


@dataclass(frozen=True)
class SourceConfig:
    name: str
    kind: Literal["sqlite", "csv_dir", "json_dir", "wikipedia"]
    path: str | None = None
    options: dict | None = None

