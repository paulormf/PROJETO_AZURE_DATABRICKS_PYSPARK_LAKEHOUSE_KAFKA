# Plano de implementação do Kinesis

## Objetivo

Adicionar um fluxo de streaming ao projeto sem alterar o pipeline batch existente:

```text
Produtor de eventos
        │
        ▼
Amazon Kinesis Data Streams
        │
        ▼
Databricks Structured Streaming
        │
        ▼
Bronze Kinesis em Parquet
        │
        ▼
Silver em Delta Lake
```

O fluxo Kinesis será independente do fluxo Azure SQL. Isso permite executar, monitorar e reprocessar cada origem separadamente.

## Configuração inicial

- Região AWS: `us-east-1`.
- Nome do stream: `lakehouse-sales-events`.
- Capacidade inicial: provisioned.
- Shards iniciais: `1`.
- Retenção inicial: `24 horas`.
- Criptografia: SSE habilitada com a chave gerenciada pela AWS.

Um shard suporta até 1 MB/s ou 1.000 registros por segundo de escrita e até 2 MB/s de leitura. A capacidade deverá ser revisada quando o volume real for conhecido.

## Evento inicial

```json
{
  "event_id": "uuid",
  "event_type": "sale_created",
  "event_time": "2026-10-23T12:00:00Z",
  "order_id": 10001,
  "sales_channel_id": 2,
  "customer_id": 501,
  "items": [
    {
      "product_id": 10,
      "quantity": 2,
      "unit_price": 49.90
    }
  ]
}
```

O campo `event_id` será usado para rastreabilidade e deduplicação. O campo `event_time` será usado para análises temporais e futura estratégia de dados atrasados.

## Segurança

As informações abaixo não devem ser colocadas no código ou no Git:

- nome ou ARN do stream em configurações sensíveis;
- access key e secret key da AWS;
- tokens;
- chaves de criptografia.

Para o primeiro teste, serão utilizadas credenciais armazenadas em um Secret Scope do Databricks. Em produção, o desenho recomendado é usar uma identidade AWS dedicada com permissões mínimas para o stream.

## Bronze Kinesis

Localização planejada:

```text
s3://lakehouse-prj-db-kafka/lakehouse/bronze/kinesis/sales-events/
```

O payload original será preservado junto com metadados técnicos:

- `partitionKey`;
- `stream`;
- `_ingested_at`;
- `_source`;
- `event_id`;
- `event_time`.

O checkpoint ficará separado dos dados:

```text
s3://lakehouse-prj-db-kafka/lakehouse/checkpoints/kinesis/sales-events/
```

## Estratégia de execução

O primeiro Job será acionado manualmente para validar conectividade, publicação e leitura. Depois, será configurado como execução incremental agendada:

```python
df_events = (
    spark.readStream
    .format("kinesis")
    .option("streamName", stream_name)
    .option("region", "us-east-1")
    .load()
)

query = (
    df_events.writeStream
    .format("parquet")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start(bronze_path)
)

query.awaitTermination()
```

Essa estratégia lê os eventos disponíveis, grava o lote e encerra a execução. O próximo agendamento continua a partir do checkpoint.

## Ordem de implementação

1. Criar o stream `lakehouse-sales-events` na região `us-east-1`.
2. Configurar um shard provisioned.
3. Habilitar criptografia no stream.
4. Criar uma identidade AWS com permissões mínimas.
5. Publicar eventos de teste.
6. Criar o notebook de ingestão Kinesis → Bronze.
7. Validar checkpoint, partição e retomada.
8. Criar a transformação Bronze Kinesis → Silver Delta.
9. Adicionar métricas de qualidade e monitoramento.
10. Adicionar o fluxo ao `databricks.yml`.

## Critérios de aceite

- eventos publicados no stream são lidos pelo Databricks;
- o payload original é preservado na Bronze;
- o checkpoint permite continuar após uma nova execução;
- eventos repetidos não geram registros duplicados na Silver;
- falhas de esquema são registradas separadamente;
- credenciais não aparecem nos logs;
- o Job termina com sucesso quando não há novos eventos;
- métricas de eventos lidos, gravados e rejeitados ficam disponíveis para auditoria.
