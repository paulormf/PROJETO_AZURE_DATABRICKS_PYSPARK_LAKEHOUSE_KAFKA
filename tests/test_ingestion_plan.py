"""Unit tests for source load classification."""

from discover.ingestion_plan import build_ingestion_plan


def test_updated_at_table_is_incremental() -> None:
    catalog = [
        {
            "schema_name": "dbo",
            "table_name": "orders",
            "row_count": 10,
            "columns": [
                {"column_name": "order_id", "is_primary_key": 1},
                {"column_name": "updated_at", "is_primary_key": 0},
            ],
        }
    ]

    plan = build_ingestion_plan(catalog)

    assert plan[0]["load_mode"] == "incremental"
    assert plan[0]["watermark_column"] == "updated_at"


def test_table_without_updated_at_is_full() -> None:
    catalog = [
        {
            "schema_name": "dbo",
            "table_name": "carriers",
            "row_count": 10,
            "columns": [{"column_name": "carrier_id", "is_primary_key": 1}],
        }
    ]

    plan = build_ingestion_plan(catalog)

    assert plan[0]["load_mode"] == "full"
    assert plan[0]["watermark_column"] is None


def test_non_business_and_control_tables_are_excluded() -> None:
    catalog = [
        {"schema_name": "cdc", "table_name": "dbo_orders_CT", "columns": []},
        {"schema_name": "dbo", "table_name": "pipeline_watermark", "columns": []},
        {"schema_name": "dbo", "table_name": "systranschemas", "columns": []},
    ]

    assert build_ingestion_plan(catalog) == []
