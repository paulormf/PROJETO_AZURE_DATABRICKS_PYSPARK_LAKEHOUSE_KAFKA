-- Alerta 1: checks de qualidade reprovados no batch Azure SQL.
SELECT
    pipeline,
    source_table,
    check_name,
    observed_value,
    checked_at
FROM main.monitoring.batch_quality
WHERE passed = false
  AND checked_at >= current_timestamp() - INTERVAL 1 DAY;

-- Alerta 2: checks de qualidade reprovados no Streaming Kinesis.
SELECT
    pipeline,
    dataset,
    check_name,
    observed_value,
    checked_at
FROM main.monitoring.streaming_quality
WHERE passed = false
  AND checked_at >= current_timestamp() - INTERVAL 1 DAY;

-- Alerta 3: falha de ingestao batch.
SELECT
    source_schema,
    source_table,
    status,
    processed_at
FROM main.monitoring.batch_ingestion_audit
WHERE status <> 'SUCCESS'
  AND processed_at >= current_timestamp() - INTERVAL 1 DAY;

-- Alerta 4: Gold de Streaming sem dados.
SELECT
    'fct_streaming_sales' AS dataset,
    COUNT(*) AS row_count
FROM main.default_gold.fct_streaming_sales
HAVING COUNT(*) = 0;

-- Alerta 5: indisponibilidade de metricas dos pipelines.
SELECT
    pipeline,
    metric_name,
    metric_value,
    observed_at
FROM main.monitoring.pipeline_summary
WHERE metric_value = false
  AND observed_at >= current_timestamp() - INTERVAL 1 DAY;
