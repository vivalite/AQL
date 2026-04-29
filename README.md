# AQL RUBICON Hermes Plugin

This repository is a Hermes Agent plugin implementing a small RUBICON-style Agentic Query Language prototype based on arXiv 2604.21413, "An Alternate Agentic AI Architecture (It's About the Data)."

The plugin exposes explicit, auditable query operations over source wrappers instead of hiding data access behind opaque agent reasoning.

## Tools

- `aql_schema`: inspect registered sources, tables, and columns.
- `aql_execute`: run AQL commands or semicolon-separated AQL scripts.
- `aql_register_source`: add SQLite, CSV directory, JSON directory, or Wikipedia sources.

## Supported AQL

```text
?
? <source>
? <table>
FIND <column(s)> FROM <table> WHERE <natural language predicate>
FIND count(*) FROM <table> WHERE <predicate>
FIND ... JOIN FIND ...
SAVE (<query>) AS <new_table>
OUTPUT <table>
DELETE <table>
```

Semicolon-separated scripts are treated as v1 compiled mode and return one visible trace for the full command sequence.

## Demo Data

On first use, each workspace gets demo sources:

- `demo_university`: SQLite university data warehouse demo.
- `demo_lab`: JSON research lab website demo.
- `wikipedia`: public MediaWiki search wrapper.

Try:

```text
FIND person_id, full_name FROM demo_university.faculty WHERE department = "Computer Science"
FIND person_id, full_name FROM demo_university.faculty WHERE title contains Professor JOIN FIND person_id, lab_role FROM demo_lab.people WHERE lab_role contains Professor
SAVE (FIND person_id, lab_role FROM demo_lab.people WHERE lab_role contains Professor) AS lab_professors
OUTPUT lab_professors
```

## Installing In Hermes

For a project-local plugin:

```bash
mkdir -p .hermes/plugins
ln -s /mnt/c/Users/vival/Desktop/AQL .hermes/plugins/aql-rubicon
HERMES_ENABLE_PROJECT_PLUGINS=1 hermes plugins enable aql-rubicon
```

For a user plugin:

```bash
mkdir -p ~/.hermes/plugins
ln -s /mnt/c/Users/vival/Desktop/AQL ~/.hermes/plugins/aql-rubicon
hermes plugins enable aql-rubicon
```

Hermes on this machine was verified against WSL Hermes Agent v0.11.0.

## Development

```bash
python -m pytest
```

The implementation intentionally avoids external runtime dependencies. `pytest` is only needed for tests.

