-- Alertas operacionais do ambiente prod.

-- Checks batch reprovados.
SELECT pipeline, source_table, check_name, observed_value, checked_at
FROM main.prod_monitoring.batch_quality
WHERE passed = false
  AND checked_at >= current_timestamp() - INTERVAL 1 DAY;

-- Checks de streaming reprovados.
SELECT pipeline, dataset, check_name, observed_value, checked_at
FROM main.prod_monitoring.streaming_quality
WHERE passed = false
  AND checked_at >= current_timestamp() - INTERVAL 1 DAY;

-- Falhas de ingestao batch.
SELECT source_schema, source_table, status, processed_at
FROM main.prod_monitoring.batch_ingestion_audit
WHERE status <> 'SUCCESS'
  AND processed_at >= current_timestamp() - INTERVAL 1 DAY;

-- Gold de streaming sem dados.
SELECT
    'fct_streaming_sales' AS dataset,
    COUNT(*) AS row_count
FROM main.prod_gold.fct_streaming_sales
HAVING COUNT(*) = 0;

-- Indisponibilidade de metricas.
SELECT pipeline, metric_name, metric_value, observed_at
FROM main.prod_monitoring.pipeline_summary
WHERE metric_value = false
  AND observed_at >= current_timestamp() - INTERVAL 1 DAY;
