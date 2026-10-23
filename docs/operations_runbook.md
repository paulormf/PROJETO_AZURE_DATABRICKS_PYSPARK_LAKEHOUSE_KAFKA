# Runbook operacional

Este guia descreve a operacao manual dos ambientes `dev` e `prod`.
Os jobs permanecem pausados por padrao no Databricks Free.

## Deploy

1. Fazer a alteracao no VS Code.
2. Executar os testes locais.
3. Criar commit e enviar para o GitHub.
4. Executar `Databricks Validate` no GitHub Actions.
5. Executar `Databricks Deploy - Dev` ou `Databricks Deploy - Prod`.

Nunca executar cargas antes de confirmar que o deploy terminou com sucesso.

## Execucao batch em dev

Executar na ordem:

1. `bronze_two_tables`.
2. `silver_two_tables`.
3. `observability`.

Validar os caminhos:

```text
s3://lakehouse-prj-db-kafka/lakehouse/dev/bronze/
s3://lakehouse-prj-db-kafka/lakehouse/dev/silver/
```

## Execucao batch em prod

Executar na ordem:

1. `test_azsql_to_s3`, para validar a conexao.
2. `bronze_two_tables`.
3. `silver_two_tables`.
4. `observability`.

Validar os schemas:

```sql
SHOW TABLES IN main.prod_silver;
SHOW TABLES IN main.prod_governed;
SHOW TABLES IN main.prod_gold;
SHOW TABLES IN main.prod_monitoring;
```

## Execucao do streaming

1. Gerar eventos no Kinesis.
2. Executar `kinesis_bronze_ingestion`.
3. Validar `main.prod_silver.kinesis_sales_events`.
4. Validar `main.prod_gold.fct_streaming_sales`.
5. Executar `observability`.

No Serverless Free, o processamento usa `AvailableNow`. Portanto, cada
execucao processa os eventos disponiveis e termina.

## Reprocessamento

Antes de reprocessar, verificar:

- caminho Bronze de origem;
- checkpoint do streaming;
- watermark da tabela batch;
- ultimo `run_id` em `prod_monitoring`.

Para uma carga batch, executar novamente o job da tabela desejada com o modo
apropriado. Para streaming, gerar novos eventos e executar novamente o job.

## Rollback de codigo

1. Identificar o commit estavel no GitHub.
2. Criar uma branch de recuperacao a partir desse commit.
3. Abrir pull request ou fazer a revisao definida pelo time.
4. Executar os workflows de validacao e deploy.
5. Reexecutar somente o job afetado.

O rollback de codigo nao remove dados do S3. Qualquer rollback de dados deve
ser planejado separadamente, usando versionamento Delta e um procedimento
aprovado.

## Validacao pos-execucao

```sql
SELECT COUNT(*) FROM main.prod_monitoring.batch_ingestion_audit;
SELECT COUNT(*) FROM main.prod_monitoring.batch_quality;
SELECT COUNT(*) FROM main.prod_monitoring.streaming_quality;
SELECT * FROM main.prod_monitoring.pipeline_summary
ORDER BY observed_at DESC;
```

Uma execucao somente deve ser considerada concluida quando o job terminar com
sucesso e as verificacoes de qualidade estiverem aprovadas.
