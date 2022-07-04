from typing import Any

import pytest


@pytest.fixture(autouse=True)
def mock_env_user(monkeypatch: Any) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "sample")
    monkeypatch.setenv("POSTGRES_USERNAME", "postgres")
    monkeypatch.setenv("POSTGRES_PASSWORD", "postgres")
    monkeypatch.setenv("FILE_PATH", "tests/test_data/test_data.json")
    # monkeypatch.setenv("FILE_PATH", "test_data/test_data.json")


@pytest.fixture
def mock_postgres_connection(monkeypatch):
    class MockCursor:
        def execute(self, *args, **kwargs):
            return None

        def executemany(self, *args, **kwargs):
            return None

    class MockConnection:
        def __init__(self, *args):
            pass

        def cursor(self, *args, **kwargs):
            return MockCursor()

        def close(self):
            return None

        def set_session(self, *args, **kwargs):
            return None

    monkeypatch.setattr("psycopg2.connect", MockConnection, raising=True)
