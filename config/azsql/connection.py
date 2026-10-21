"""Connection factory for Azure SQL.

Credentials are read from environment variables and must not be committed to
the repository. For local development, a .env file can be used.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import pyodbc
from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class AzureSqlSettings:
    """Settings required to connect to an Azure SQL database."""

    server: str
    database: str
    username: str
    password: str
    driver: str = "ODBC Driver 18 for SQL Server"

    @classmethod
    def from_environment(cls) -> "AzureSqlSettings":
        """Build settings from AZSQL_* environment variables."""

        required = {
            "server": os.getenv("AZSQL_SERVER"),
            "database": os.getenv("AZSQL_DATABASE"),
            "username": os.getenv("AZSQL_USERNAME"),
            "password": os.getenv("AZSQL_PASSWORD"),
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            variables = ", ".join(f"AZSQL_{name.upper()}" for name in missing)
            raise ValueError(f"Missing required environment variables: {variables}")

        return cls(
            server=required["server"],
            database=required["database"],
            username=required["username"],
            password=required["password"],
            driver=os.getenv("AZSQL_DRIVER", cls.driver),
        )

    def connection_string(self) -> str:
        """Return an ODBC connection string for Azure SQL."""

        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER=tcp:{self.server},1433;"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )

    def connect(self) -> pyodbc.Connection:
        """Open and return a database connection."""

        return pyodbc.connect(self.connection_string())
