CREATE SCHEMA IF NOT EXISTS main.monitoring;

CREATE TABLE IF NOT EXISTS main.monitoring.batch_ingestion_audit
USING DELTA
LOCATION 's3://lakehouse-prj-db-kafka/lakehouse/control/observability/batch_ingestion_audit/';

CREATE TABLE IF NOT EXISTS main.monitoring.batch_quality
USING DELTA
LOCATION 's3://lakehouse-prj-db-kafka/lakehouse/control/observability/batch_quality/';

CREATE TABLE IF NOT EXISTS main.monitoring.streaming_quality
USING DELTA
LOCATION 's3://lakehouse-prj-db-kafka/lakehouse/control/observability/streaming_quality/';

CREATE TABLE IF NOT EXISTS main.monitoring.pipeline_summary
USING DELTA
LOCATION 's3://lakehouse-prj-db-kafka/lakehouse/control/observability/pipeline_summary/';
