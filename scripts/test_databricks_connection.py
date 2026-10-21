import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from databricks_client import get_spark_session, get_databricks_config, get_sql_connection


def test_connection():
    print("Testando conexao com Databricks...")
    print("-" * 50)
    
    try:
        config = get_databricks_config()
        print(f"[OK] Configuracao carregada:")
        print(f"  Host: {config.host}")
        print(f"  Cluster ID: {config.cluster_id or 'Nao definido'}")
        print(f"  Warehouse ID: {config.warehouse_id or 'Nao definido'}")
        print(f"  Catalog: {config.catalog}")
        print(f"  Schema: {config.schema}")
    except Exception as e:
        print(f"[ERRO] Erro ao carregar configuracao: {e}")
        return False
    
    if config.warehouse_id:
        print("\nTestando conexao via SQL Warehouse (versao free)...")
        try:
            connection = get_sql_connection()
            print(f"[OK] Conexao SQL estabelecida com sucesso")
            
            cursor = connection.cursor()
            cursor.execute("SELECT current_database()")
            result = cursor.fetchone()
            print(f"  Database atual: {result[0] if result else 'N/A'}")
            
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()
            print(f"  Databases disponiveis: {len(databases)}")
            for db in databases[:5]:
                print(f"    - {db[0]}")
            if len(databases) > 5:
                print(f"    ... e mais {len(databases) - 5} databases")
            
            cursor.close()
            connection.close()
            print(f"[OK] Conexao fechada com sucesso")
        except Exception as e:
            print(f"[ERRO] Erro na conexao SQL: {e}")
            import traceback
            traceback.print_exc()
            return False
    elif config.cluster_id:
        print("\nTestando conexao via Spark Connect (cluster)...")
        try:
            spark = get_spark_session(app_name="TestConnection")
            print(f"[OK] SparkSession criada com sucesso")
            print(f"  Versao Spark: {spark.version}")
            
            databases = spark.catalog.listDatabases()
            print(f"  Databases disponiveis: {len(databases)}")
            for db in databases[:5]:
                print(f"    - {db.name}")
            if len(databases) > 5:
                print(f"    ... e mais {len(databases) - 5} databases")
        except Exception as e:
            print(f"[ERRO] Erro ao criar SparkSession: {e}")
            return False
    else:
        print("\n[ERRO] Nem DATABRICKS_WAREHOUSE_ID nem DATABRICKS_CLUSTER_ID estao definidos")
        print("  Para versao free: configure DATABRICKS_WAREHOUSE_ID")
        print("  Para versao paid: configure DATABRICKS_CLUSTER_ID")
        return False
    
    print("-" * 50)
    print("[OK] Todos os testes passaram!")
    return True


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
