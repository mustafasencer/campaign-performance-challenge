import json
import os

import typer

from utils.database import create_connection


def gen(file_name):
    with open(file_name) as fh:
        while line := fh.readline():
            yield json.loads(line)


def process_file(cur, progress, filepath):
    """

    :param cur:
    :param progress:
    :param filepath:
    :return:
    """
    # read json data line by line
    for row in gen(filepath):
        print(row)


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
