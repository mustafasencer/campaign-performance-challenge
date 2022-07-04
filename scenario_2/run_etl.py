import json
import os
from datetime import datetime
from typing import Any, Dict, Final, Iterator, Tuple

import typer
from click._termui_impl import ProgressBar
from psycopg2._psycopg import cursor

from utils.database import execute

from .migrate_db import (
    customer_table_insert,
    event_table_insert,
    fact_table_table_insert,
    fact_table_table_select,
    user_table_insert,
)


def read_line(file_name: str) -> Iterator[Dict[str, Any]]:
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


def process_file(cur: cursor, progress: "ProgressBar[int]") -> None:
    """

    :param cur:
    :param progress:
    :return:
    """
    # read json data line by line
    for row in read_line(os.getenv("FILE_PATH", default="")):
        if not all(key in row.keys() for key in EVENT_COLUMNS):
            typer.echo("File does not contain all required columns!")
            continue

        row["fired_at"] = datetime.strptime(row["fired_at"], "%d/%m/%Y, %H:%M:%S")
        execute(
            cur,
            user_table_insert,
            [row["user_id"], row["email"], row["ip"]],
        )

        execute(
            cur,
            customer_table_insert,
            [row["customer_id"]],
        )

        execute(
            cur,
            event_table_insert,
            [
                row["event_id"],
                row["customer_id"],
                row["user_id"],
                row["fired_at"],
                row["event_type"],
            ],
        )

        date_hour = row["fired_at"].replace(minute=0, second=0, microsecond=0)

        execute(cur, fact_table_table_select, [date_hour, row["customer_id"]])

        fact_table_row = cur.fetchone()
        if fact_table_row is None:
            fact_table_row = {
                "date_hour": date_hour,
                "customer_id": row["customer_id"],
                "page_loads": 1 if row["event_type"] == "ReferralPageLoad" else 0,
                "clicks": 1 if row["event_type"] == "ReferralRecommendClick" else 0,
                "unique_user_clicks": None,
                "click_through_rate": None,
            }
        else:
            fact_table_row["page_loads"] = (
                fact_table_row["page_loads"] + 1
                if row["event_type"] == "ReferralPageLoad"
                else fact_table_row["page_loads"]
            )
            fact_table_row["clicks"] = (
                fact_table_row["clicks"] + 1
                if row["event_type"] == "ReferralRecommendClick"
                else 0
            )
            fact_table_row["unique_user_clicks"] = None
            fact_table_row["click_through_rate"] = None

        execute(cur, fact_table_table_insert, list(fact_table_row.values()))

        print(row)
        print(row)
        print(row)
