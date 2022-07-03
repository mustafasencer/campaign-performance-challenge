import json

import pandas as pd
import psycopg2
import numpy
from psycopg2.extensions import register_adapter, AsIs
from .sql_queries import *


def adapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


register_adapter(numpy.float64, adapt_numpy_float64)
register_adapter(numpy.int64, adapt_numpy_int64)


def batch_insert():
    pass


def gen(file_name):
    with open(file_name) as fh:
        while line := fh.readline():
            yield json.loads(line)


def process_file(cur, conn, filepath):
    # read json data
    pass


def run():
    """
    Driver function for loading songs and log data into Postgres database
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sample user=postgres password=postgres")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    try:
        process_file(cur, conn, filepath="data/aklamio_challenge.json")
    finally:
        conn.close()
