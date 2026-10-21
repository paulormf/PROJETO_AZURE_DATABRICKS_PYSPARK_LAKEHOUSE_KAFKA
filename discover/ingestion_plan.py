"""Build the source ingestion plan from the Azure SQL catalog."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


EXCLUDED_TABLES = {"pipeline_watermark", "systranschemas"}


def build_ingestion_plan(catalog: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Classify business tables as incremental or full loads.

    The project deliberately uses only ``updated_at`` as an incremental
    watermark. Other date-like columns do not change the classification.
    """

    plan = []
    for table in catalog:
        if table["schema_name"] != "dbo":
            continue
        if table["table_name"] in EXCLUDED_TABLES:
            continue

        columns = table.get("columns", [])
        column_names = {column["column_name"] for column in columns}
        primary_keys = [
            column["column_name"]
            for column in columns
            if column.get("is_primary_key") == 1
        ]
        is_incremental = "updated_at" in column_names

        plan.append(
            {
                "source_schema": table["schema_name"],
                "source_table": table["table_name"],
                "row_count_at_discovery": table.get("row_count", 0),
                "load_mode": "incremental" if is_incremental else "full",
                "watermark_column": "updated_at" if is_incremental else None,
                "primary_keys": primary_keys,
                "bronze_format": "parquet",
                "silver_format": "delta",
                "gold_format": "delta",
            }
        )

    return sorted(plan, key=lambda item: item["source_table"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--catalog",
        type=Path,
        default=Path("artifacts/azure_sql_catalog.json"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/ingestion_plan.json"),
    )
    args = parser.parse_args()

    catalog = json.loads(args.catalog.read_text(encoding="utf-8"))
    plan = build_ingestion_plan(catalog)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(plan, indent=2), encoding="utf-8")
    print(f"Generated ingestion plan for {len(plan)} tables: {args.output}")


if __name__ == "__main__":
    main()
