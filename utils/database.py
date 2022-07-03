import os
from typing import List, Optional, Tuple

import numpy
import psycopg2
from psycopg2.extensions import AsIs, register_adapter


def adapt_numpy_float64(numpy_float64):
    """

    :param numpy_float64:
    :return:
    """
    return AsIs(numpy_float64)


def adapt_numpy_int64(numpy_int64):
    """

    :param numpy_int64:
    :return:
    """
    return AsIs(numpy_int64)


register_adapter(numpy.float64, adapt_numpy_float64)
register_adapter(numpy.int64, adapt_numpy_int64)


def create_connection(database_name: str):
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


def create_database(database_name: str):
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
    return create_connection(os.getenv("POSTGRES_DB"))


def chunks(data, chunk_size):
    """
    Generate chunks from input **data** with size(s) of **chunk_size**
    :param data:
    :param chunk_size:
    :return:
    """
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


def execute(cur, query: str, vars: Optional[Tuple] = None):
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


def execute_many(cur, query: str, vars_list: List):
    """

    :param cur:
    :param query:
    :param vars_list:
    :return:
    """
    for batch in chunks(vars_list, 1000):
        cur.executemany(query, batch)


def drop_tables(cur, conn, table_list):
    """
    Run's all the drop table queries defined in sql_queries.py
    :param table_list:
    :param cur: cursor to the database
    :param conn: database connection reference
    """
    for query in table_list:
        cur.execute(query)


def create_tables(cur, conn, table_list):
    """
    Run's all the create table queries defined in sql_queries.py
    :param table_list:
    :param cur: cursor to the database
    :param conn: database connection reference
    """
    for query in table_list:
        cur.execute(query)
