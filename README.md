# Azure Databricks PySpark Lakehouse Kafka

Projeto de Engenharia de Dados ponta a ponta, desenvolvido para demonstrar
práticas de ingestão, transformação, qualidade, orquestração e processamento
de dados em batch e streaming.

## Objetivos

- Integrar fontes relacionais, APIs REST e eventos de streaming.
- Implementar arquitetura Lakehouse com camadas Bronze, Silver e Gold.
- Desenvolver pipelines com Python, PySpark e SQL.
- Implementar cargas iniciais e incrementais com controle de watermark.
- Utilizar Delta Lake para armazenamento e transformação de dados.
- Orquestrar pipelines com Apache Airflow e Azure Data Factory.
- Processar eventos com Apache Kafka e Spark Structured Streaming.
- Implementar testes automatizados com Pytest.
- Aplicar Git Flow e integração contínua com GitHub Actions.

## Fontes de dados

- Azure SQL Server: fonte transacional principal.
- Oracle: fonte relacional complementar.
- API REST: integração com dados externos.
- Kafka: transporte de eventos para processamento em streaming.

## Arquitetura lógica

1. Ingestão das fontes.
2. Bronze: armazenamento dos dados recebidos.
3. Silver: limpeza, padronização e validação.
4. Gold: modelos analíticos e produtos de dados.
5. Consumo por consultas SQL e ferramentas analíticas.

## Tecnologias

- Python e SQL
- PySpark
- Azure SQL e Oracle
- Azure Data Lake Storage Gen2
- Azure Data Factory
- Azure Databricks
- Delta Lake
- Apache Airflow
- Apache Kafka
- Pytest
- Git e GitHub Actions

## Qualidade e confiabilidade

O projeto terá testes automatizados, logs, tratamento de erros,
regras de qualidade, rastreabilidade e mecanismos para reexecução segura.

## Segurança

Credenciais e strings de conexão devem ser configuradas por variáveis
de ambiente ou mecanismos de gerenciamento de segredos. Nunca devem
ser versionadas no repositório.

## Status

Em desenvolvimento.
