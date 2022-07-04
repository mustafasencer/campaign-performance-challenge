import os
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, Final, List

import typer
from click._termui_impl import ProgressBar
from psycopg2._psycopg import cursor

from scenario_1st import run_etl as run_etl_1
from scenario_1st.migrate_db import create_table_queries as create_1
from scenario_1st.migrate_db import drop_table_queries as drop_1
from scenario_2nd import run_etl as run_etl_2
from scenario_2nd.migrate_db import create_table_queries as create_2
from scenario_2nd.migrate_db import drop_table_queries as drop_2
from utils.database import (
    create_connection,
    create_database,
    create_tables,
    drop_tables,
)

app = typer.Typer(help="Aklamio ETL CLI")


@dataclass
class _Scenario:
    process_file: Callable[[cursor, "ProgressBar[int]"], None]
    create_table_queries: List[str]
    drop_table_queries: List[str]


class Scenario(str, Enum):
    FIRST = "1st"
    SECOND = "2nd"


SCENARIOS_MAPPER: Final[Dict[Scenario, _Scenario]] = {
    Scenario.FIRST: _Scenario(run_etl_1.process_file, create_1, drop_1),
    Scenario.SECOND: _Scenario(run_etl_2.process_file, create_2, drop_2),
}


def check_env_variables() -> None:
    ENV_VARS = [
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USERNAME",
        "POSTGRES_PASSWORD",
        "FILE_PATH",
    ]
    missing = set(ENV_VARS) - set(os.environ)
    if missing:
        typer.echo(
            f"Please define the following environment variables before command execution: {missing}"
        )
        raise typer.Exit(code=1)


@app.command()
def run(scenario: Scenario = Scenario.FIRST) -> None:
    """
    Run the ETL pipeline with the given scenario.
    """
    check_env_variables()
    conn, cur = create_connection(os.getenv("POSTGRES_DB", default="postgres"))
    process_file = SCENARIOS_MAPPER[scenario].process_file

    try:
        with typer.progressbar(length=100) as progress:
            process_file(cur, progress)
    finally:
        conn.close()

    typer.echo("ETL terminated successfully!")


@app.command()
def migrate(scenario: Scenario = Scenario.FIRST) -> None:
    """
    Migrate database schemas for the given scenario.
    """
    check_env_variables()
    conn, cur = create_database(os.getenv("POSTGRES_DB", default="postgres"))

    try:
        drop_tables(cur, SCENARIOS_MAPPER[scenario].drop_table_queries)
        print("Tables dropped successfully!")

        create_tables(cur, SCENARIOS_MAPPER[scenario].create_table_queries)
        print("Tables created successfully!")
    finally:
        conn.close()

    typer.echo("Migration terminated successfully!")


if __name__ == "__main__":
    app()
