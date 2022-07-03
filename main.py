import os
from enum import Enum

import typer

from scenario_1 import create_tables as migrate_first
from scenario_1 import etl as run_first
from scenario_2 import create_tables as migrate_second
from scenario_2 import etl as run_second

app = typer.Typer(help="Aklamio ETL CLI")


def check_env_variables():
    ENV_VARS = ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USERNAME", "POSTGRES_PASSWORD"]
    missing = set(ENV_VARS) - set(os.environ)
    if missing:
        typer.echo(f"Environment variables do not exist: {missing}")
        raise typer.Exit(code=1)


class Scenario(str, Enum):
    first = "1st"
    second = "2nd"


@app.command()
def run(scenario: Scenario = Scenario.first):
    """
    Run the ETL pipeline with the given scenario.
    """
    check_env_variables()
    if scenario.value == Scenario.first.value:
        run_first.run()
    else:
        run_second.run()
    typer.echo("ETL terminated successfully!")


@app.command()
def migrate(scenario: Scenario = Scenario.first):
    """
    Migrate database schemas for the given scenario.
    """
    check_env_variables()
    if scenario.value == Scenario.first.value:
        migrate_first.migrate()
    else:
        migrate_second.migrate()
    typer.echo("Migration terminated successfully!")


if __name__ == "__main__":
    app()
