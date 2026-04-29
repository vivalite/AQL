"""JSON schemas exposed to Hermes for AQL RUBICON tools."""

from __future__ import annotations


AQL_EXECUTE = {
    "name": "aql_execute",
    "description": (
        "Execute RUBICON AQL against registered data sources. Supports FIND/FROM/WHERE, "
        "JOIN, SAVE, OUTPUT, DELETE, and semicolon-separated scripts."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "AQL command or semicolon-separated AQL script to execute.",
            },
            "workspace": {
                "type": "string",
                "description": "Workspace name used for source configs and saved tables.",
                "default": "default",
            },
            "limit": {
                "type": "integer",
                "description": "Maximum rows returned per command.",
                "default": 50,
                "minimum": 1,
                "maximum": 500,
            },
        },
        "required": ["query"],
    },
}


AQL_SCHEMA = {
    "name": "aql_schema",
    "description": (
        "Inspect RUBICON AQL schemas. target omitted lists sources; a source name lists "
        "tables; a table name lists columns."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "target": {
                "type": "string",
                "description": "Optional source or table target. Leave empty for all sources.",
            },
            "workspace": {
                "type": "string",
                "description": "Workspace name used for source configs and saved tables.",
                "default": "default",
            },
        },
    },
}


AQL_REGISTER_SOURCE = {
    "name": "aql_register_source",
    "description": "Register or update an AQL data source in a workspace.",
    "parameters": {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": "Unique source name, e.g. university_dw.",
            },
            "kind": {
                "type": "string",
                "enum": ["sqlite", "csv_dir", "json_dir", "wikipedia"],
                "description": "Wrapper type used to query the source.",
            },
            "path": {
                "type": "string",
                "description": "Filesystem path for sqlite/csv_dir/json_dir sources. Optional for wikipedia.",
            },
            "workspace": {
                "type": "string",
                "description": "Workspace name used for source configs and saved tables.",
                "default": "default",
            },
            "options": {
                "type": "object",
                "description": "Optional wrapper-specific settings.",
                "additionalProperties": True,
            },
        },
        "required": ["name", "kind"],
    },
}

