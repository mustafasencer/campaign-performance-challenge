from typer.testing import CliRunner

from main import app

runner = CliRunner()


def test_app() -> None:
    result = runner.invoke(app, ["run", "--scenario", "1st"])
    assert result.exit_code == 0
    assert "ETL terminated successfully!" in result.stdout


def test_migrate_command() -> None:
    result = runner.invoke(app, ["migrate", "--scenario", "1st"])
    assert result.exit_code == 0
    assert "Tables dropped successfully!" in result.stdout


def test_run_command() -> None:
    result = runner.invoke(app, ["run", "--scenario", "1st"])
    assert result.exit_code == 0
    assert "ETL terminated successfully!" in result.stdout


def test_app_with_unsupported_scenario_option() -> None:
    result = runner.invoke(app, ["run", "--scenario", "unsupported_option"])
    assert result.exit_code == 2
    assert (
        "Error: Invalid value for '--scenario': 'unsupported_option' is not one of '1st', '2nd'."
        in result.stdout
    )


def test_app_with_undefined_environment_variables(monkeypatch) -> None:
    monkeypatch.delenv("POSTGRES_HOST", raising=True)
    result = runner.invoke(app, ["run", "--scenario", "1st"])
    assert result.exit_code == 1
    assert (
        "Please define the following environment variables before command execution"
        in result.stdout
    )
