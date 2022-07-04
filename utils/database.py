import os
from typing import Any, List, Optional, Tuple

import numpy
import numpy as np
import psycopg2
from psycopg2._psycopg import connection, cursor
from psycopg2.extensions import AsIs, register_adapter


def adapt_numpy_float64(numpy_float64: np.float64) -> AsIs:
    """

    :param numpy_float64:
    :return:
    """
    return AsIs(numpy_float64)


def adapt_numpy_int64(numpy_int64: np.float64) -> AsIs:
    """

    :param numpy_int64:
    :return:
    """
    return AsIs(numpy_int64)


register_adapter(numpy.float64, adapt_numpy_float64)
register_adapter(numpy.int64, adapt_numpy_int64)


def create_connection(database_name: str) -> Tuple[connection, cursor]:
    """

    :param database_name:
    :return:
    """
    conn = psycopg2.connect(
        f"host={os.getenv('POSTGRES_HOST')} port={os.getenv('POSTGRES_PORT')} dbname={database_name} user={os.getenv('POSTGRES_USERNAME')} password={os.getenv('POSTGRES_PASSWORD')}"
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    return conn, cur


def create_database(database_name: str) -> Tuple[connection, cursor]:
    """
    Establishes database connection and return's the connection and cursor references.
    :param database_name:
    :return: return's (cur, conn) a cursor and connection reference
    """
    # connect to default database
    conn, cur = create_connection("postgres")

    try:
        cur.execute(
            f"""SELECT pg_terminate_backend(pg_stat_activity.pid)
                        FROM pg_stat_activity
                        WHERE pg_stat_activity.datname = '{database_name}' AND pid <> pg_backend_pid();"""
        )
        cur.execute("DROP DATABASE IF EXISTS sample")
        # create sample database with UTF8 encoding
        cur.execute("CREATE DATABASE sample WITH ENCODING 'utf8' TEMPLATE template0")
    finally:
        # close connection to default database
        conn.close()

    # connect to sample database
    return create_connection(os.getenv("POSTGRES_DB", default="postgres"))


def execute(cur: cursor, query: str, vars: Optional[List[Any]] = None) -> None:
    """

    :param cur:
    :param query:
    :param vars:
    :return:
    """
    if vars is None:
        cur.execute(query)
    else:
        cur.execute(query, vars)


def execute_many(cur: cursor, query: str, vars_list: List[List[Any]]) -> None:
    """

    :param cur:
    :param query:
    :param vars_list:
    :return:
    """
    cur.executemany(query, vars_list)


def drop_tables(cur: cursor, table_list: List[str]) -> None:
    """
    Run's all the drop table queries defined in migrate_db.py
    :param table_list:
    :param cur: cursor to the database
    :param conn: database connection reference
    """
    for query in table_list:
        cur.execute(query)


def create_tables(cur: cursor, table_list: List[str]) -> None:
    """
    Run's all the create table queries defined in migrate_db.py
    :param table_list:
    :param cur: cursor to the database
    :param conn: database connection reference
    """
    for query in table_list:
        cur.execute(query)
