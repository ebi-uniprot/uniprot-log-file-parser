import os.path
from duckdb import connect, DuckDBPyConnection, CatalogException


def get_db_connection():
    return connect()


def is_log_already_saved_as_parquets(
    dbc: DuckDBPyConnection, meta_path: str, log_path: str
):
    if not os.path.isfile(meta_path):
        return False
    return bool(
        dbc.sql(
            f"SELECT log_path FROM read_csv_auto('{meta_path}', header=True) WHERE log_path = '{log_path}'"
        )
    )
