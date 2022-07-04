import os

from typer.testing import CliRunner

from main import app
from scenario_2nd.migrate_db import fact_table_table_select
from utils.database import create_connection, execute

runner = CliRunner()

DATA_CHECK = [
    {
        "query_params": ["2022-01-01 00:00:00", "32"],
        "page_loads": 38,
        "clicks": 3,
        "unique_user_clicks": 3,
        "click_through_rate": 0,
    },
    {
        "query_params": ["2022-01-01 00:00:00", "344"],
        "page_loads": 47,
        "clicks": 7,
        "unique_user_clicks": 7,
        "click_through_rate": 0,
    },
    {
        "query_params": ["2022-01-01 01:00:00", "29"],
        "page_loads": 41,
        "clicks": 7,
        "unique_user_clicks": 7,
        "click_through_rate": 0,
    },
    {
        "query_params": ["2022-01-01 01:00:00", "126"],
        "page_loads": 35,
        "clicks": 4,
        "unique_user_clicks": 4,
        "click_through_rate": 0,
    },
]


def test_1st_scenario():
    result = runner.invoke(app, ["migrate", "--scenario", "1st"])
    assert result.exit_code == 0

    result = runner.invoke(app, ["run", "--scenario", "1st"])
    assert result.exit_code == 0

    assert "ETL terminated successfully!" in result.stdout


def test_2nd_scenario():
    result = runner.invoke(app, ["migrate", "--scenario", "2nd"])
    assert result.exit_code == 0

    result = runner.invoke(app, ["run", "--scenario", "2nd"])
    assert result.exit_code == 0

    assert "ETL terminated successfully!" in result.stdout


def test_data_integrity():
    scenarios = ["1st"]

    for scenario in scenarios:
        runner.invoke(app, ["migrate", "--scenario", scenario])
        runner.invoke(app, ["run", "--scenario", scenario])
        conn, cur = create_connection(os.getenv("POSTGRES_DB"))
        for data in DATA_CHECK:
            execute(cur, fact_table_table_select, data["query_params"])
            result = cur.fetchone()
            assert result["page_loads"] == data["page_loads"]
            assert result["clicks"] == data["clicks"]
            assert result["unique_user_clicks"] == data["unique_user_clicks"]
            assert result["click_through_rate"] == data["click_through_rate"]
