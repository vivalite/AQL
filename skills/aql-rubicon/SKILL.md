# AQL RUBICON

Use this skill when a user asks Hermes to query or reconcile data across registered sources with the AQL RUBICON plugin.

## Workflow

1. Inspect available sources first with `aql_schema`.
2. Inspect source tables and table columns before writing a query.
3. Write explicit AQL. Prefer:
   - `FIND <columns> FROM <table> WHERE <predicate>`
   - `FIND ... JOIN FIND ...` when two sources need integration
   - `SAVE (<query>) AS <table>` for intermediate results the user may inspect or reuse
4. Keep natural-language predicates narrow and deterministic, such as `department = "Computer Science"`, `title contains Professor`, or `promotion_date after 2020-01-01`.
5. Report the returned rows, provenance, and trace. Do not hide intermediate tables behind narrative reasoning.

## Examples

```text
? demo_university
? demo_university.faculty
FIND person_id, full_name FROM demo_university.faculty WHERE department = "Computer Science"
SAVE (FIND person_id, lab_role FROM demo_lab.people WHERE lab_role contains Professor) AS lab_professors
OUTPUT lab_professors
FIND person_id, full_name FROM demo_university.faculty WHERE title contains Professor JOIN FIND person_id, lab_role FROM demo_lab.people WHERE lab_role contains Professor
```

## Boundaries

This plugin uses deterministic adapters only. It does not translate arbitrary prose into SQL with an LLM. If a predicate returns unexpected rows, inspect the schema and rewrite the predicate more explicitly.

