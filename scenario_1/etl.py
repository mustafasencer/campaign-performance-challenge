import pandas as pd
import typer

from utils.database import *

from .sql_queries import *


def process_file(cur, progress, filepath):
    """

    :param cur:
    :param progress:
    :param filepath:
    :return:
    """
    # read json data
    df = pd.read_json(filepath, lines=True)
    progress.update(20)

    # data deduplication and sorting
    df["date_hour"] = pd.to_datetime(
        pd.to_datetime(df["fired_at"]).dt.strftime("%Y-%m-%d %H:00:00")
    )
    df.sort_values("fired_at", inplace=True)
    df.drop_duplicates(subset="event_id", keep="last", inplace=True)

    # load users table
    user_df = df[["user_id", "email", "ip"]]
    user_df.drop_duplicates(subset="user_id", keep="last", inplace=True)
    execute_many(cur, user_table_insert, user_df.values.tolist())
    progress.update(10)

    # load customers table
    customer_df = df[["customer_id"]]
    execute_many(cur, customer_table_insert, customer_df.values.tolist())
    progress.update(10)

    # load events table
    event_df = df[["event_id", "customer_id", "user_id", "fired_at", "event_type"]]
    execute_many(cur, event_table_insert, event_df.values.tolist())
    progress.update(30)

    group = ["date_hour", "customer_id"]
    time_idx = pd.date_range(df["date_hour"].min(), df["date_hour"].max(), freq="60min")

    # load fact table
    fact_df = pd.DataFrame(
        index=pd.MultiIndex.from_product(
            [time_idx, df["customer_id"].unique()], names=["date_hour", "customer_id"]
        )
    )

    fact_df["page_loads"] = (
        df[df["event_type"] == "ReferralPageLoad"].groupby(group).size()
    )
    fact_df["clicks"] = (
        df[df["event_type"] == "ReferralRecommendClick"].groupby(group).size()
    )
    fact_df["unique_user_clicks"] = (
        df[df["event_type"] == "ReferralRecommendClick"]
            .groupby(group)["user_id"]
            .nunique()
    )
    fact_df = fact_df.fillna(0).astype(int)
    fact_df["click_through_rate"] = fact_df["clicks"] / fact_df["page_loads"]
    fact_df.reset_index(inplace=True)

    execute_many(cur, fact_table_table_insert, fact_df.values.tolist())
    progress.update(20)


def run():
    """
    Driver function for extracting, transforming and loading the campaign performance analysis
    :return:
    """
    conn, cur = create_connection(os.getenv("POSTGRES_DB"))

    try:
        with typer.progressbar(length=100) as progress:
            process_file(cur, progress, filepath="data/aklamio_challenge.json")
    finally:
        conn.close()
