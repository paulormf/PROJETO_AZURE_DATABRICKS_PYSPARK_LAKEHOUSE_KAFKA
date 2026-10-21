import os
from typing import Optional, Union
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class DatabricksConfig:
    host: str
    token: str
    cluster_id: Optional[str] = None
    warehouse_id: Optional[str] = None
    http_path: Optional[str] = None
    catalog: str = "hive_metastore"
    schema: str = "default"

    @classmethod
    def from_env(cls) -> "DatabricksConfig":
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")
        
        if not host:
            raise ValueError("DATABRICKS_HOST não definido nas variáveis de ambiente")
        if not token:
            raise ValueError("DATABRICKS_TOKEN não definido nas variáveis de ambiente")
        
        return cls(
            host=host,
            token=token,
            cluster_id=os.getenv("DATABRICKS_CLUSTER_ID"),
            warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            catalog=os.getenv("DATABRICKS_CATALOG", "hive_metastore"),
            schema=os.getenv("DATABRICKS_SCHEMA", "default"),
        )


def get_databricks_config() -> DatabricksConfig:
    return DatabricksConfig.from_env()


def get_spark_session(
    app_name: str = "DatabricksLakehouseApp",
    config: Optional[DatabricksConfig] = None,
) -> "pyspark.sql.SparkSession":
    from pyspark.sql import SparkSession
    
    if config is None:
        config = get_databricks_config()
    
    if not config.cluster_id:
        raise ValueError(
            "DATABRICKS_CLUSTER_ID é necessário para databricks-connect. "
            "Para versão free do Databricks, use get_sql_connection() em vez disso."
        )
    
    spark_builder = SparkSession.builder.appName(app_name)
    
    spark_builder = spark_builder.remote(
        f"databricks://{config.host}",
        token=config.token,
        cluster_id=config.cluster_id,
    )
    
    spark = spark_builder.getOrCreate()
    
    spark.conf.set("spark.sql.catalogImplementation", "hive")
    spark.conf.set("spark.databricks.catalog.catalog", config.catalog)
    spark.conf.set("spark.databricks.catalog.schema", config.schema)
    
    return spark


def get_sql_connection(config: Optional[DatabricksConfig] = None):
    import databricks.sql as sql
    
    if config is None:
        config = get_databricks_config()
    
    if not config.warehouse_id:
        raise ValueError("DATABRICKS_WAREHOUSE_ID é necessário para conexão SQL")
    
    # Se warehouse_id já tiver o caminho completo, usar diretamente
    if config.warehouse_id.startswith("/sql/"):
        http_path = config.warehouse_id
    else:
        http_path = config.http_path or f"/sql/1.0/warehouses/{config.warehouse_id}"
    
    connection = sql.connect(
        server_hostname=config.host,
        http_path=http_path,
        access_token=config.token,
    )
    
    return connection
