from utils.database import *

from .sql_queries import create_table_queries, drop_table_queries


def migrate():
    """
    Driver function for the migration operations
    :return:
    """
    conn, cur = create_database(os.getenv("POSTGRES_DB"))

    try:
        drop_tables(cur, conn, drop_table_queries)
        print("Table dropped successfully!")

        create_tables(cur, conn, create_table_queries)
        print("Table created successfully!")
    finally:
        conn.close()
