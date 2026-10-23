-- Consultas de observabilidade do ambiente prod.

-- Execucoes batch por status.
SELECT status, COUNT(*) AS executions
FROM main.prod_monitoring.batch_ingestion_audit
GROUP BY status
ORDER BY status;

-- Ultima execucao batch por tabela.
SELECT
    source_schema,
    source_table,
    bronze_rows,
    silver_rows,
    rows_removed,
    status,
    processed_at
FROM main.prod_monitoring.batch_ingestion_audit
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY source_schema, source_table
    ORDER BY processed_at DESC
) = 1
ORDER BY processed_at DESC;

-- Checks batch reprovados.
SELECT pipeline, source_table, check_name, observed_value, checked_at
FROM main.prod_monitoring.batch_quality
WHERE passed = false
ORDER BY checked_at DESC;

-- Qualidade do streaming.
SELECT dataset, check_name, passed, observed_value, checked_at
FROM main.prod_monitoring.streaming_quality
ORDER BY checked_at DESC;

-- Disponibilidade dos pipelines.
SELECT pipeline, metric_name, metric_value, observed_at
FROM main.prod_monitoring.pipeline_summary
ORDER BY observed_at DESC;
