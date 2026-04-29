# AQL RUBICON Hermes Plugin

This repository is a Hermes Agent plugin implementing a small RUBICON-style Agentic Query Language prototype based on arXiv 2604.21413, "An Alternate Agentic AI Architecture (It's About the Data)."

The plugin exposes explicit, auditable query operations over source wrappers instead of hiding data access behind opaque agent reasoning.

## Tools

- `aql_schema`: inspect registered sources, tables, and columns.
- `aql_execute`: run AQL commands or semicolon-separated AQL scripts.
- `aql_register_source`: add SQLite, CSV directory, JSON directory, or Wikipedia sources.
- `aql_explain`: parse and resolve AQL without executing it.
- `aql_sources`: list, show, status-check, enable, disable, or delete sources.

## Supported AQL

```text
?
? <source>
? <table>
FIND <column(s)> FROM <table> WHERE <natural language predicate>
FIND count(*) FROM <table> WHERE <predicate>
FIND ... JOIN FIND ...
FIND ... JOIN FIND ... ON left_col = right_col
EXPLAIN <query>
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
FIND person_id, full_name FROM demo_university.faculty WHERE title contains Professor JOIN FIND person_id, lab_role FROM demo_lab.people WHERE lab_role contains Professor ON person_id = person_id
SAVE (FIND person_id, lab_role FROM demo_lab.people WHERE lab_role contains Professor) AS lab_professors
OUTPUT lab_professors
```

## Installing In Hermes

The repo root is the plugin directory. If you are using Hermes from WSL and this repo is on Windows at `C:\Users\vival\Desktop\AQL`, the WSL path is:

```bash
/mnt/c/Users/vival/Desktop/AQL
```

### User Plugin Install

This is the simplest install path. It makes the plugin available to every Hermes project for your WSL user.

```bash
mkdir -p ~/.hermes/plugins
ln -sfn /mnt/c/Users/vival/Desktop/AQL ~/.hermes/plugins/aql-rubicon
~/.local/bin/hermes plugins enable aql-rubicon
~/.local/bin/hermes plugins list
```

Then start Hermes:

```bash
~/.local/bin/hermes
```

Inside Hermes, verify the plugin is loaded:

```text
/plugins
```

You should see `aql-rubicon` with the tools `aql_execute`, `aql_schema`, `aql_register_source`, `aql_explain`, and `aql_sources`.

### Project-Local Plugin Install

Use this if you only want the plugin available in one project. Project plugins are disabled by default in Hermes, so enable project plugin discovery when starting Hermes.

```bash
mkdir -p .hermes/plugins
ln -sfn /mnt/c/Users/vival/Desktop/AQL .hermes/plugins/aql-rubicon
HERMES_ENABLE_PROJECT_PLUGINS=1 ~/.local/bin/hermes plugins enable aql-rubicon
HERMES_ENABLE_PROJECT_PLUGINS=1 ~/.local/bin/hermes
```

Hermes on this machine was verified against WSL Hermes Agent v0.11.0.

## Usage Examples

Hermes calls the tools for you. In a Hermes session, use prompts like the examples below.

### 1. Inspect Available Sources

```text
Use aql_schema to list available AQL sources.
```

Expected sources on first run:

- `demo_university`
- `demo_lab`
- `wikipedia`

Inspect the tables in one source:

```text
Use aql_schema with target demo_university.
```

Inspect columns for a table:

```text
Use aql_schema with target demo_university.faculty.
```

Equivalent AQL schema commands are:

```text
?
? demo_university
? demo_university.faculty
```

### 2. Run A Basic Query

```text
Use aql_execute with:
FIND person_id, full_name FROM demo_university.faculty WHERE department = "Computer Science"
```

The tool returns JSON with:

- `columns`
- `rows`
- `row_count`
- `provenance`
- `trace`
- `truncated`
- `errors`

### 3. Join Two Sources

This joins the SQLite university demo source with the JSON research lab demo source on the shared `person_id` column.

```text
Use aql_execute with:
FIND person_id, full_name FROM demo_university.faculty WHERE title = "Professor" JOIN FIND person_id, lab_role FROM demo_lab.people WHERE lab_role = "Research Lab Professor" ON person_id = person_id
```

The trace will include an `explicit_join` step so you can see which columns were used and how many rows were joined.

If `ON` is omitted, AQL falls back to a natural join over same-named columns:

```text
Use aql_execute with:
FIND person_id, full_name FROM demo_university.faculty WHERE title = "Professor" JOIN FIND person_id, lab_role FROM demo_lab.people WHERE lab_role = "Research Lab Professor"
```

Use explicit joins when source columns have different names:

```text
FIND person_id, full_name FROM source_a.people JOIN FIND researcher_id, lab_role FROM source_b.roster ON person_id = researcher_id
```

If the right side has a duplicate non-join column, the result prefixes it with the right table name, such as `roster.full_name`.

### 3.1 Explain A Query Without Running It

```text
Use aql_explain with:
FIND person_id FROM demo_university.faculty WHERE department = "Computer Science" JOIN FIND person_id FROM demo_lab.people WHERE lab_role contains Professor ON person_id = person_id
```

You can also use AQL syntax directly:

```text
Use aql_execute with:
EXPLAIN FIND person_id FROM demo_university.faculty WHERE department = "Computer Science"
```

Explain results include resolved sources, table names, selected columns, predicate plans, join plans, and whether SQLite pushdown is available.

### 4. Save And Reuse An Intermediate Table

```text
Use aql_execute with:
SAVE (FIND person_id, lab_role FROM demo_lab.people WHERE lab_role contains Professor) AS lab_professors
```

Then inspect it:

```text
Use aql_execute with:
OUTPUT lab_professors
```

Delete it when done:

```text
Use aql_execute with:
DELETE lab_professors
```

You can also run this as one semicolon-separated script:

```text
Use aql_execute with:
SAVE (FIND person_id, lab_role FROM demo_lab.people WHERE lab_role contains Professor) AS lab_professors; OUTPUT lab_professors
```

Semicolon-separated scripts are reported as `compiled` mode in the result payload.

### 5. Aggregates

```text
Use aql_execute with:
FIND count(*) FROM demo_university.faculty WHERE department = "Computer Science"
```

Supported aggregates:

- `count(*)`
- `count(column)`
- `sum(column)`
- `avg(column)`
- `min(column)`
- `max(column)`

For SQLite sources, supported projections, predicates, limits, and aggregates are pushed into SQL. The trace includes `pushdown: true` and the SQL statement used.

### 6. Register Your Own Sources

SQLite:

```text
Use aql_register_source with name my_dw, kind sqlite, and path /home/wei/data/my.db.
```

Then query it:

```text
Use aql_execute with:
FIND * FROM my_dw.some_table WHERE status = "active"
```

CSV directory:

```text
Use aql_register_source with name support_csv, kind csv_dir, and path /home/wei/data/support_csv.
```

Each `.csv` file becomes a table named after the file stem. For example, `/home/wei/data/support_csv/tickets.csv` becomes `support_csv.tickets`.

JSON directory:

```text
Use aql_register_source with name lab_json, kind json_dir, and path /home/wei/data/lab_json.
```

Each `.json` file must contain either a list of objects or an object with a `rows` list. Each file becomes a table named after the file stem.

Wikipedia:

```text
Use aql_register_source with name wiki, kind wikipedia.
```

Then query:

```text
Use aql_execute with:
FIND title, url, snippet FROM wiki.pages WHERE programming languages
```

Manage existing sources:

```text
Use aql_sources with action list.
Use aql_sources with action status and name demo_university.
Use aql_sources with action disable and name demo_lab.
Use aql_sources with action enable and name demo_lab.
Use aql_sources with action delete and name my_old_source.
```

Disabled sources remain visible in `aql_sources list`, but AQL ignores them during query table resolution.

### 7. Predicate Style

Predicates are deterministic and intentionally simple. Prefer explicit filters:

```text
department = "Computer Science"
title contains Professor
opened_year >= 2000
promotion_date after 2020-01-01
status != "closed"
department IN ("Computer Science", Mathematics)
opened_year BETWEEN 1980 AND 2000
closed_at IS NULL
closed_at IS NOT NULL
```

Clauses can be combined with `AND`, `OR`, and parentheses:

```text
department = "Computer Science" AND title contains Professor
(department = "Computer Science" OR department = Mathematics) AND title contains Professor
```

If the predicate is plain text, the wrapper falls back to keyword matching across row values.

## State And Workspaces

State is stored under:

```text
~/.hermes/aql-rubicon/<workspace>/
```

Each workspace has its own:

- source registry
- saved intermediate tables
- execution traces

All tools accept a `workspace` parameter. If omitted, the workspace is `default`.

Example:

```text
Use aql_execute in workspace research_demo with:
FIND * FROM demo_lab.projects WHERE status = "active"
```

## Troubleshooting

If Hermes cannot find the plugin, check the symlink:

```bash
ls -la ~/.hermes/plugins/aql-rubicon
```

If `hermes` is not on your WSL `PATH`, use the full path:

```bash
~/.local/bin/hermes plugins list
```

If the plugin is installed but not loaded, enable it:

```bash
~/.local/bin/hermes plugins enable aql-rubicon
```

If you are using project-local plugins, remember to set:

```bash
export HERMES_ENABLE_PROJECT_PLUGINS=1
```

If a table name is ambiguous across sources, use the qualified name:

```text
demo_university.faculty
demo_lab.people
```

If a predicate returns unexpected rows, inspect the table schema and rewrite the predicate more explicitly. This plugin does not use LLM text-to-SQL translation.

## Development

```bash
uv run --with pytest --no-project python -m pytest
```

The implementation intentionally avoids external runtime dependencies. `pytest` is only needed for tests.
