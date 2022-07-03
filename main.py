import os

import typer

from scenario_1 import create_tables as migrate_1, etl as run_1
from scenario_2 import create_tables as migrate_2, etl as run_2

app = typer.Typer(help="Aklimio ETL CLI")
ENV_VARS = ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USERNAME", "POSTGRES_PASSWORD"]
missing = set(ENV_VARS) - set(os.environ)
if missing:
    print("Environment variables do not exist: %s" % missing)


@app.command()
def run(scenario: int):
    """
    Run the ETL pipeline with the given scenario.
    """
    if scenario == 1:
        run_1.run()
    elif scenario == 2:
        run_2.run()
    else:
        typer.echo("Scenario not found!")


@app.command()
def migrate(scenario: int):
    """
    Migrate database schemas for the given scenario.
    """
    if scenario == 1:
        migrate_1.migrate()
    elif scenario == 2:
        migrate_2.main()
    else:
        typer.echo("Scenario not found!")


if __name__ == "__main__":
    app()
