# AQL RUBICON

Use this skill when a user asks Hermes to query or reconcile data across registered sources with the AQL RUBICON plugin.

## Workflow

1. Inspect available sources first with `aql_schema`.
2. Inspect source tables and table columns before writing a query.
3. Use `aql_explain` before expensive, multi-source, or ambiguous queries.
4. Write explicit AQL. Prefer:
   - `FIND <columns> FROM <table> WHERE <predicate>`
   - `FIND ... JOIN FIND ... ON left_col = right_col` when two sources need integration
   - `SAVE (<query>) AS <table>` for intermediate results the user may inspect or reuse
5. Use `aql_sources` for source status, enable/disable, and deletion workflows.
6. Keep predicates narrow and deterministic, such as `department = "Computer Science"`, `title contains Professor`, `department IN ("Computer Science", Mathematics)`, or `promotion_date after 2020-01-01`.
7. Report the returned rows, provenance, and trace. Do not hide intermediate tables behind narrative reasoning.

## Examples

```text
? demo_university
? demo_university.faculty
EXPLAIN FIND person_id, full_name FROM demo_university.faculty WHERE department = "Computer Science"
FIND person_id, full_name FROM demo_university.faculty WHERE department = "Computer Science"
SAVE (FIND person_id, lab_role FROM demo_lab.people WHERE lab_role contains Professor) AS lab_professors
OUTPUT lab_professors
FIND person_id, full_name FROM demo_university.faculty WHERE title contains Professor JOIN FIND person_id, lab_role FROM demo_lab.people WHERE lab_role contains Professor ON person_id = person_id
```

## Boundaries

This plugin uses deterministic adapters only. It does not translate arbitrary prose into SQL with an LLM. If a predicate returns unexpected rows, inspect the schema and rewrite the predicate more explicitly.
