-- KPI: execucoes batch por status
SELECT
    status,
    COUNT(*) AS executions
FROM main.monitoring.batch_ingestion_audit
GROUP BY status
ORDER BY status;

-- KPI: ultimas execucoes batch por tabela
SELECT
    source_schema,
    source_table,
    bronze_rows,
    silver_rows,
    rows_removed,
    status,
    processed_at
FROM main.monitoring.batch_ingestion_audit
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY source_schema, source_table
    ORDER BY processed_at DESC
) = 1
ORDER BY processed_at DESC;

-- Qualidade: checks reprovados
SELECT
    pipeline,
    source_table,
    check_name,
    observed_value,
    checked_at
FROM main.monitoring.batch_quality
WHERE passed = false
ORDER BY checked_at DESC;

-- Streaming: qualidade e metricas observadas
SELECT
    dataset,
    check_name,
    passed,
    observed_value,
    checked_at
FROM main.monitoring.streaming_quality
ORDER BY checked_at DESC;

-- Disponibilidade dos pipelines
SELECT
    pipeline,
    metric_name,
    metric_value,
    observed_at
FROM main.monitoring.pipeline_summary
ORDER BY observed_at DESC;
