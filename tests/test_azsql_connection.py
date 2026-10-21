"""Unit tests for Azure SQL configuration."""

import pytest

from config.azsql.connection import AzureSqlSettings


def test_from_environment_requires_connection_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    for variable in (
        "AZSQL_SERVER",
        "AZSQL_DATABASE",
        "AZSQL_USERNAME",
        "AZSQL_PASSWORD",
    ):
        monkeypatch.delenv(variable, raising=False)

    with pytest.raises(ValueError, match="AZSQL_SERVER"):
        AzureSqlSettings.from_environment()


def test_connection_string_uses_encrypted_sql_server_connection() -> None:
    settings = AzureSqlSettings(
        server="example.database.windows.net",
        database="demo",
        username="user",
        password="secret",
    )

    connection_string = settings.connection_string()

    assert "SERVER=tcp:example.database.windows.net,1433" in connection_string
    assert "DATABASE=demo" in connection_string
    assert "Encrypt=yes" in connection_string
    assert "TrustServerCertificate=no" in connection_string
