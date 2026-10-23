# Checklist de prontidao para producao

Este documento registra os itens necessarios antes de considerar o pipeline pronto
para operacao continua. No Databricks Free, `prod` e uma separacao logica dentro
do mesmo workspace.

## Fluxos validados

- [x] Azure SQL para Bronze Parquet.
- [x] Bronze para Silver Delta.
- [x] Silver para Governed no Unity Catalog.
- [x] Gold com dbt.
- [x] Kinesis para Bronze Parquet.
- [x] Kinesis Bronze para Silver Delta.
- [x] Monitoramento batch e streaming.
- [x] Dashboard e consultas de producao.
- [x] Deploy por GitHub Actions.

## Antes da operacao continua

- [ ] Definir o horario de execucao dos jobs batch.
- [ ] Definir a frequencia do processamento Kinesis.
- [ ] Manter os jobs pausados no ambiente Free ate a demonstracao.
- [ ] Configurar alertas de falha e qualidade.
- [ ] Validar as consultas do dashboard apos cada alteracao de schema.

## Seguranca e governanca

- [x] Segredos fora do codigo-fonte.
- [x] Credenciais AWS armazenadas em secret scope.
- [x] Views Governed sem exposicao direta de dados sensiveis.
- [ ] Migrar para grupos e permissoes Unity Catalog em um workspace pago.
- [ ] Substituir tokens pessoais por identidade gerenciada ou service principal.
- [ ] Revisar rotacao e expiracao de credenciais.

## Qualidade e reconciliacao

- [x] Contagem Bronze versus Silver.
- [x] Verificacao de chave primaria nula.
- [x] Verificacao de duplicidade na Silver.
- [x] Testes dbt na Gold.
- [x] Testes automatizados no CI.
- [ ] Definir limites de alerta para volume e atraso.
- [ ] Criar dados de teste controlados para regressao.

## Operacao e custos

- [ ] Definir retencao para Bronze, checkpoints e auditorias.
- [ ] Avaliar particionamento para tabelas maiores.
- [ ] Avaliar compactacao e manutencao Delta.
- [ ] Monitorar tempo de execucao e consumo do SQL Warehouse.
- [ ] Definir limite de custo e procedimento de desligamento.

## Recuperacao e rollback

- [x] Codigo versionado no GitHub.
- [x] Deploy reproduzivel via Databricks Asset Bundles.
- [ ] Documentar como retornar para um commit anterior.
- [ ] Testar reprocessamento de uma tabela Bronze.
- [ ] Testar reprocessamento de um lote Kinesis.
- [ ] Definir procedimento para restaurar tabelas Gold.

## Limitacoes atuais do ambiente Free

- `dev` e `prod` compartilham o mesmo workspace Databricks.
- Nao ha isolamento real de identidade, rede e computacao entre ambientes.
- Grupos e permissoes corporativas podem nao estar disponiveis.
- Streaming usa micro-lotes `AvailableNow`, nao processamento continuo.
- Os jobs de producao permanecem pausados ate uma demonstracao controlada.
