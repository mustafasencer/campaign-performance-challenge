import os

from typer.testing import CliRunner

from main import app
from scenario_2nd.migrate_db import fact_table_table_select
from utils.database import create_connection, execute

runner = CliRunner()

DATA_CHECK = [
    {
        "query_params": ["2022-01-01 00:00:00", "32"],
        "1st": {
            "page_loads": 38,
            "clicks": 3,
            "unique_user_clicks": 3,
            "click_through_rate": 0,
        },
        "2nd": {
            "page_loads": 50,
            "clicks": 3,
            "unique_user_clicks": 3,
            "click_through_rate": 0,
        },
    },
    {
        "query_params": ["2022-01-01 00:00:00", "344"],
        "1st": {
            "page_loads": 47,
            "clicks": 7,
            "unique_user_clicks": 7,
            "click_through_rate": 2,
        },
        "2nd": {
            "page_loads": 64,
            "clicks": 7,
            "unique_user_clicks": 7,
            "click_through_rate": 2,
        },
    },
    {
        "query_params": ["2022-01-01 01:00:00", "29"],
        "1st": {
            "page_loads": 39,
            "clicks": 7,
            "unique_user_clicks": 7,
            "click_through_rate": 3,
        },
        "2nd": {
            "page_loads": 52,
            "clicks": 7,
            "unique_user_clicks": 7,
            "click_through_rate": 4,
        },
    },
    {
        "query_params": ["2022-01-01 01:00:00", "126"],
        "1st": {
            "page_loads": 32,
            "clicks": 4,
            "unique_user_clicks": 4,
            "click_through_rate": 0,
        },
        "2nd": {
            "page_loads": 43,
            "clicks": 4,
            "unique_user_clicks": 4,
            "click_through_rate": 0,
        },
    },
    {
        "query_params": ["2022-01-01 02:00:00", "54"],
        "1st": {
            "page_loads": 38,
            "clicks": 5,
            "unique_user_clicks": 5,
            "click_through_rate": 1,
        },
        "2nd": {
            "page_loads": 54,
            "clicks": 5,
            "unique_user_clicks": 5,
            "click_through_rate": 1,
        },
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
    scenarios = ["1st", "2nd"]

    for scenario in scenarios:
        runner.invoke(app, ["migrate", "--scenario", scenario])
        runner.invoke(app, ["run", "--scenario", scenario])
        conn, cur = create_connection(os.getenv("POSTGRES_DB"))
        for data in DATA_CHECK:
            execute(cur, fact_table_table_select, data["query_params"])
            result = cur.fetchone()
            values_to_be_validated = data[scenario]
            assert result["page_loads"] == values_to_be_validated["page_loads"]
            assert result["clicks"] == values_to_be_validated["clicks"]
            assert (
                result["unique_user_clicks"]
                == values_to_be_validated["unique_user_clicks"]
            )
            assert (
                result["click_through_rate"]
                == values_to_be_validated["click_through_rate"]
            )
