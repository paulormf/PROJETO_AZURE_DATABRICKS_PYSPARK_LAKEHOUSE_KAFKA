"""Discover tables, columns, keys and approximate row counts in Azure SQL."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from config.azsql.connection import AzureSqlSettings


TABLES_QUERY = """
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    SUM(CASE WHEN p.index_id IN (0, 1) THEN p.rows ELSE 0 END) AS row_count
FROM sys.tables AS t
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
LEFT JOIN sys.partitions AS p ON p.object_id = t.object_id
GROUP BY s.name, t.name
ORDER BY s.name, t.name;
"""

COLUMNS_QUERY = """
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    c.column_id,
    c.name AS column_name,
    TYPE_NAME(c.user_type_id) AS data_type,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    c.is_identity,
    c.is_computed,
    CASE WHEN pk.column_id IS NULL THEN 0 ELSE 1 END AS is_primary_key
FROM sys.tables AS t
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
JOIN sys.columns AS c ON c.object_id = t.object_id
LEFT JOIN (
    SELECT ic.object_id, ic.column_id
    FROM sys.indexes AS i
    JOIN sys.index_columns AS ic
        ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    WHERE i.is_primary_key = 1
) AS pk ON pk.object_id = c.object_id AND pk.column_id = c.column_id
ORDER BY s.name, t.name, c.column_id;
"""


def _rows_to_dicts(cursor: Any) -> list[dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def discover_catalog(settings: AzureSqlSettings) -> list[dict[str, Any]]:
    """Return catalog metadata grouped by table."""

    with settings.connect() as connection:
        tables_cursor = connection.cursor().execute(TABLES_QUERY)
        tables = _rows_to_dicts(tables_cursor)

        columns_cursor = connection.cursor().execute(COLUMNS_QUERY)
        columns = _rows_to_dicts(columns_cursor)

    columns_by_table: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for column in columns:
        key = (column.pop("schema_name"), column.pop("table_name"))
        columns_by_table.setdefault(key, []).append(column)

    catalog = []
    for table in tables:
        key = (table["schema_name"], table["table_name"])
        catalog.append({**table, "columns": columns_by_table.get(key, [])})
    return catalog


def write_catalog(catalog: Iterable[dict[str, Any]], output: Path) -> None:
    """Write the discovered catalog as indented JSON."""

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(list(catalog), indent=2, default=str),
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/azure_sql_catalog.json"),
        help="JSON file where the catalog will be saved.",
    )
    args = parser.parse_args()

    catalog = discover_catalog(AzureSqlSettings.from_environment())
    write_catalog(catalog, args.output)
    print(f"Discovered {len(catalog)} tables. Catalog saved to {args.output}.")


if __name__ == "__main__":
    main()
