import json
import os
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Final, Iterator, Tuple

import dateutil.parser as parser
import typer
from click._termui_impl import ProgressBar
from psycopg2._psycopg import cursor

from utils.database import execute

from .migrate_db import (
    customer_table_insert,
    event_table_select,
    event_table_upsert,
    fact_table_table_clicks_upsert,
    fact_table_table_page_loads_upsert,
    fact_table_table_select,
    unique_user_table_insert,
    unique_user_table_select,
    user_table_insert,
)


def read_line(file_name: str) -> Iterator[Dict[str, Any]]:
    """
    Generator func to read the JSON file line by line
    :param file_name:
    :return:
    """
    with open(file_name) as file:
        for line in file:
            yield json.loads(line)


EVENT_COLUMNS: Final[Tuple[str, ...]] = (
    "event_id",
    "fired_at",
    "customer_id",
    "user_id",
    "event_type",
    "ip",
    "email",
)

CLICK_THROUGH_RATE_TIME_WINDOW: Final[int] = 30


class EventType(str, Enum):
    CLICK = "ReferralRecommendClick"
    PAGE_LOAD = "ReferralPageLoad"


def get_page_loads(fact_table_row: Dict[str, Any]) -> Any:
    """
    Get page loads
    :param fact_table_row:
    :return:
    """
    if fact_table_row is None:
        return 1
    return fact_table_row["page_loads"] + 1


def get_clicks(fact_table_row: Dict[str, Any]) -> Any:
    """
    Get clicks
    :param fact_table_row:
    :return:
    """
    if fact_table_row is None:
        return 1
    return fact_table_row["clicks"] + 1


def get_unique_user_clicks(
    cur: cursor, date_hour: datetime, line_row: Dict[str, Any]
) -> Any:
    """
    Get unique user clicks
    :param cur:
    :param date_hour:
    :param line_row:
    :return:
    """
    execute(
        cur,
        unique_user_table_insert,
        [date_hour, line_row["customer_id"], line_row["user_id"]],
    )
    execute(cur, unique_user_table_select, [date_hour, line_row["customer_id"]])
    result = cur.fetchone()
    return result["count"]


def get_click_through_rate(
    cur: cursor, fact_table_row: Dict[str, Any], line_row: Dict[str, Any]
) -> Any:
    """
    Get click through rate
    :param cur:
    :param fact_table_row:
    :param line_row:
    :return:
    """
    execute(
        cur,
        event_table_select,
        [
            line_row["customer_id"],
            line_row["user_id"],
            EventType.PAGE_LOAD.value,
            line_row["fired_at"] - timedelta(minutes=CLICK_THROUGH_RATE_TIME_WINDOW),
        ],
    )
    exists = cur.rowcount
    if fact_table_row is None:
        return 1 if exists else 0
    return (
        fact_table_row["click_through_rate"] + 1
        if exists
        else fact_table_row["click_through_rate"]
    )


def process_file(cur: cursor, _: "ProgressBar[int]") -> None:
    """
    Process the input json file (defined path is retrieved from the env variable `FILE_PATH`) for 2nd scenario,
     loads the file line by line and process each line accordingly.
    :param cur:
    :param _:
    :return: None
    """
    # read json data line by line
    for line_row in read_line(os.getenv("FILE_PATH", default="")):
        if not all(key in line_row.keys() for key in EVENT_COLUMNS):
            typer.echo("File does not contain all required columns!")
            continue

        line_row["fired_at"] = parser.parse(line_row["fired_at"])
        execute(
            cur,
            user_table_insert,
            [line_row["user_id"], line_row["email"], line_row["ip"]],
        )

        execute(
            cur,
            customer_table_insert,
            [line_row["customer_id"]],
        )

        execute(
            cur,
            event_table_upsert,
            [
                line_row["event_id"],
                line_row["customer_id"],
                line_row["user_id"],
                line_row["fired_at"],
                line_row["event_type"],
            ],
        )

        date_hour = line_row["fired_at"].replace(minute=0, second=0, microsecond=0)

        execute(cur, fact_table_table_select, [date_hour, line_row["customer_id"]])

        fact_table_row = cur.fetchone()

        if line_row["event_type"] == EventType.PAGE_LOAD.value:
            page_loads = get_page_loads(fact_table_row)
            execute(
                cur,
                fact_table_table_page_loads_upsert,
                {
                    "date_hour": date_hour,
                    "customer_id": line_row["customer_id"],
                    "page_loads": page_loads,
                },
            )

        elif line_row["event_type"] == EventType.CLICK.value:
            clicks = get_clicks(fact_table_row)
            unique_user_clicks = get_unique_user_clicks(cur, date_hour, line_row)
            click_through_rate = get_click_through_rate(cur, fact_table_row, line_row)
            execute(
                cur,
                fact_table_table_clicks_upsert,
                {
                    "date_hour": date_hour,
                    "customer_id": line_row["customer_id"],
                    "clicks": clicks,
                    "unique_user_clicks": unique_user_clicks,
                    "click_through_rate": click_through_rate,
                },
            )
