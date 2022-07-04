from typing import Any

from typer.testing import CliRunner

from main import app

runner = CliRunner()


def test_app(mock_postgres_connection):
    result = runner.invoke(app, ["run", "--scenario", "1st"])
    assert result.exit_code == 0
    assert "ETL terminated successfully!" in result.stdout


def test_migrate_command(mock_postgres_connection):
    result = runner.invoke(app, ["migrate", "--scenario", "1st"])
    assert result.exit_code == 0
    assert "Tables dropped successfully!" in result.stdout


def test_run_command(mock_postgres_connection):
    result = runner.invoke(app, ["run", "--scenario", "1st"])
    assert result.exit_code == 0
    assert "ETL terminated successfully!" in result.stdout


def test_run_command_without_scenario(mock_postgres_connection):
    result = runner.invoke(app, ["run"])
    assert result.exit_code == 0
    assert "ETL terminated successfully!" in result.stdout


def test_run_command_with_corrupt_data(monkeypatch, mock_postgres_connection):
    monkeypatch.setenv("FILE_PATH", "tests/test_data/corrupt_test_data.json")

    scenarios = [
        {
            "name": "1st",
            "exit_code": 1,
            "stdout": "File does not contain all required columns!",
        },
        {
            "name": "2nd",
            "exit_code": 0,
            "stdout": "File does not contain all required columns!",
        },
    ]

    for scenario in scenarios:
        result = runner.invoke(app, ["run", "--scenario", scenario["name"]])
        assert result.exit_code == scenario["exit_code"]
        assert scenario["stdout"] in result.stdout


def test_app_with_unsupported_scenario_option(mock_postgres_connection):
    result = runner.invoke(app, ["run", "--scenario", "unsupported_option"])
    assert result.exit_code == 2
    assert (
        "Error: Invalid value for '--scenario': 'unsupported_option' is not one of '1st', '2nd'."
        in result.stdout
    )


def test_app_with_undefined_environment_variables(
    monkeypatch: Any, mock_postgres_connection
):
    monkeypatch.delenv("POSTGRES_HOST", raising=True)
    result = runner.invoke(app, ["run", "--scenario", "1st"])
    assert result.exit_code == 1
    assert (
        "Please define the following environment variables before command execution"
        in result.stdout
    )
