import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from databricks_client import get_sql_connection


def example_query():
    """Exemplo de como executar queries SQL no Databricks (versao free)"""
    
    print("Conectando ao Databricks...")
    connection = get_sql_connection()
    cursor = connection.cursor()
    
    try:
        # Listar databases
        print("\n--- Databases disponiveis ---")
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        for db in databases:
            print(f"  - {db[0]}")
        
        # Listar tabelas no database atual
        print("\n--- Tabelas no database atual ---")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        for table in tables:
            print(f"  - {table[1]} (temp: {table[2]})")
        
        # Exemplo de query SELECT
        print("\n--- Query de exemplo ---")
        cursor.execute("SELECT current_database(), current_user(), current_timestamp()")
        result = cursor.fetchone()
        print(f"  Database: {result[0]}")
        print(f"  User: {result[1]}")
        print(f"  Timestamp: {result[2]}")
        
    finally:
        cursor.close()
        connection.close()
        print("\nConexao fechada.")


if __name__ == "__main__":
    example_query()
