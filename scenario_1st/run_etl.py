import os
from enum import Enum
from typing import Final, List, Tuple

import pandas as pd
import typer
from click._termui_impl import ProgressBar
from psycopg2._psycopg import cursor

from utils.database import execute_many

from .migrate_db import (
    customer_table_insert,
    event_table_insert,
    fact_table_table_insert,
    user_table_insert,
)

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


def get_page_loads(df: pd.DataFrame, group: List[str]) -> pd.Series:
    return df[df["event_type"] == EventType.PAGE_LOAD.value].groupby(group).size()


def get_clicks(df: pd.DataFrame, group: List[str]) -> pd.Series:
    return df[df["event_type"] == EventType.CLICK.value].groupby(group).size()


def get_unique_user_clicks(df: pd.DataFrame, group: List[str]) -> pd.Series:
    return (
        df[df["event_type"] == EventType.CLICK.value]
        .groupby(group)["user_id"]
        .nunique()
    )


def get_click_through_rate(df: pd.DataFrame, group: List[str]) -> pd.Series:
    df["fired_at_1_diff"] = (
        df.groupby(["customer_id", "user_id"])["fired_at"]
        .diff()
        .fillna(pd.Timedelta(seconds=0))
        .dt.seconds
        / 60
    ).astype(int)
    df["event_type_1_shifted"] = df.groupby(["customer_id", "user_id"])[
        "event_type"
    ].shift()
    condition = (
        (df["event_type"] == EventType.CLICK.value)
        & (df["fired_at_1_diff"] < CLICK_THROUGH_RATE_TIME_WINDOW)
        & (df["event_type_1_shifted"] == EventType.PAGE_LOAD.value)
    )
    return df[condition].groupby(group).size().fillna(0).astype(int)


def process_file(cur: cursor, progress: "ProgressBar[int]") -> None:
    """
    Process the input json file (defined path is retrieved from the env variable `FILE_PATH`) for 1st scenario,
     loads the entire file and process accordingly.
    :param cur:
    :param progress:
    :return: None
    """
    # read json data
    df = pd.read_json(os.getenv("FILE_PATH"), lines=True)
    progress.update(20)

    if not all(column in df.columns for column in EVENT_COLUMNS):
        typer.echo("File does not contain all required columns!")
        raise typer.Exit(code=1)

    # data deduplication and sorting
    df.sort_values("fired_at", inplace=True)
    df.drop_duplicates(subset="event_id", keep="last", inplace=True)

    # load users table
    user_df = df[["user_id", "email", "ip"]]
    user_df = user_df.drop_duplicates(subset="user_id", keep="last")
    execute_many(cur, user_table_insert, user_df.values.tolist())
    progress.update(10)

    # load customers table
    customer_df = df[["customer_id"]]
    execute_many(cur, customer_table_insert, customer_df.values.tolist())
    progress.update(10)

    # load events table
    event_df = df[["event_id", "customer_id", "user_id", "fired_at", "event_type"]]
    execute_many(cur, event_table_insert, event_df.values.tolist())
    progress.update(40)

    # create a column storing fired_at at only a hourly precision
    df["date_hour"] = df["fired_at"].dt.floor(freq="h")

    # Create the hourly sliding time horizon for fact_table based on the min and max values of date_hour
    time_idx = pd.date_range(df["date_hour"].min(), df["date_hour"].max(), freq="60min")

    # create a new Dataframe having the cross product of date_hour and customer_id columns as index
    fact_df = pd.DataFrame(
        index=pd.MultiIndex.from_product(
            [time_idx, df["customer_id"].unique()], names=["date_hour", "customer_id"]
        )
    )

    group = ["date_hour", "customer_id"]

    fact_df["page_loads"] = get_page_loads(df, group)
    fact_df["clicks"] = get_clicks(df, group)
    fact_df["unique_user_clicks"] = get_unique_user_clicks(df, group)
    fact_df["click_through_rate"] = get_click_through_rate(df, group)

    fact_df = fact_df.fillna(0).astype(int)
    fact_df.reset_index(inplace=True)
    progress.update(10)

    # load fact_table table
    execute_many(cur, fact_table_table_insert, fact_df.values.tolist())
    progress.update(10)
