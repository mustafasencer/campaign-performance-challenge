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
    user_table_insert,
)


def read_line(file_name: str) -> Iterator[Dict[str, Any]]:
    with open(file_name) as fh:
        while line := fh.readline():
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

        date_hour = datetime.strptime(row["fired_at"], "%d/%m/%Y, %H:%M:%S")
        date_hour = date_hour.replace(minute=0, second=0, microsecond=0)

        execute(
            cur,
            user_table_insert,
            [value for key, value in row.items() if key in ("user_id", "email", "ip")],
        )

        execute(
            cur,
            customer_table_insert,
            [value for key, value in row.items() if key == "customer_id"],
        )

        execute(
            cur,
            event_table_insert,
            [
                value
                for key, value in row.items()
                if key
                in ("event_id", "customer_id", "user_id", "fired_at", "event_type")
            ],
        )

        row["page_loads"] = None
        row["clicks"] = None
        row["unique_user_clicks"] = None
        row["click_through_rate"] = None

        execute(cur, fact_table_table_insert)

        print(row)
        print(row)
        print(row)
