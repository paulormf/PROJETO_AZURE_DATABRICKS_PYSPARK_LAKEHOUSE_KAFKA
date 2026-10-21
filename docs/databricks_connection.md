# Conexão com Databricks

Este projeto suporta duas formas de conexão com o Databricks:

## Versão Free (SQL Warehouse)

Para a versão free do Databricks, use a conexão SQL direta:

### Configuração

1. Copie `.env.example` para `.env`
2. Preencha as variáveis:
   ```bash
   DATABRICKS_HOST=https://xxx.cloud.databricks.com
   DATABRICKS_TOKEN=seu_token_aqui
   DATABRICKS_WAREHOUSE_ID=seu_warehouse_id
   DATABRICKS_CATALOG=hive_metastore
   DATABRICKS_SCHEMA=default
   ```

### Como encontrar o Warehouse ID

1. Acesse seu workspace Databricks
2. Vá em SQL > SQL Warehouses
3. Clique no warehouse que deseja usar
4. O ID está na URL: `/sql/warehouses/<warehouse_id>`

### Uso

```python
from databricks_client import get_sql_connection, get_databricks_config

# Conectar via SQL
connection = get_sql_connection()
cursor = connection.cursor()

# Executar query
cursor.execute("SELECT * FROM my_table LIMIT 10")
results = cursor.fetchall()

# Fechar conexão
cursor.close()
connection.close()
```

### Testar conexão

No Windows (com virtualenv ativado):
```bash
.venv\Scripts\python.exe scripts\test_databricks_connection.py
```

No Linux/Mac:
```bash
python scripts/test_databricks_connection.py
```

## Versão Paid (Cluster)

Para a versão paga com clusters, use Spark Connect:

### Configuração

1. Copie `.env.example` para `.env`
2. Preencha as variáveis:
   ```bash
   DATABRICKS_HOST=https://xxx.cloud.databricks.com
   DATABRICKS_TOKEN=seu_token_aqui
   DATABRICKS_CLUSTER_ID=seu_cluster_id
   DATABRICKS_CATALOG=hive_metastore
   DATABRICKS_SCHEMA=default
   ```

### Como encontrar o Cluster ID

1. Acesse seu workspace Databricks
2. Vá em Compute > Clusters
3. Clique no cluster que deseja usar
4. O ID está na URL: `/#setting/clusters/<cluster_id>/configuration`

### Uso

```python
from databricks_client import get_spark_session

# Criar SparkSession conectada ao cluster
spark = get_spark_session(app_name="MyApp")

# Usar como PySpark normal
df = spark.read.table("my_table")
df.show()

# Fechar sessão
spark.stop()
```

## Gerar Personal Access Token (PAT)

1. Acesse: `https://<workspace-url>/settings/personal-access-tokens`
2. Clique em "Generate new token"
3. Dê um nome e selecione a lifetime
4. Copie o token gerado e salve no `.env`

## Segurança

- Nunca commit o arquivo `.env`
- O `.gitignore` já está configurado para ignorar arquivos de credenciais
- Use tokens com permissões mínimas necessárias
- Revogue tokens quando não precisar mais deles
