from duckdb import connect, DuckdbcPyConnection
from pandas import DataFrame


def get_dbc_connection(db_path: str):
    return connect(database=db_path, read_only=False)


def setup_tables(dbc: DuckdbcPyConnection, namespace: str):
    dbc.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {namespace}(
            datetime TIMESTAMP WITH TIME ZONE,
            method VARCHAR,
            request VARCHAR,
            status BIGINT,
            bytes BIGINT,
            referrer VARCHAR,
            useragent VARCHAR,
        )
        """
    )
    dbc.execute(
        """
        CREATE TABLE IF NOT EXISTS
            insertedlogs(
                sha512hash VARCHAR PRIMARY KEY,
                n_lines_imported BIGINT,
                n_lines_skipped BIGINT
            )
        """
    )


def insert_log_data(
    dbc: DuckdbcPyConnection,
    namespace: str,
    log_df: DataFrame  # pylint: disable=unused-argument
):
    dbc.execute(f"INSERT INTO {namespace} SELECT * FROM log_df")


def insert_log_meta(
    dbc: DuckdbcPyConnection,
    sha512hash: str,
    n_lines_imported: int,
    n_lines_skipped: int
):
    dbc.execute(
        "INSERT INTO logmeta VALUES "
        f"('{sha512hash}', {n_lines_imported}, {n_lines_skipped})"
    )


def is_log_already_inserted(dbc: DuckdbcPyConnection, sha512hash: str):
    results = dbc.execute(
        f"SELECT COUNT(*) FROM insertedlogs WHERE sha512hash = '{sha512hash}'"
    )
    return bool(results.fetchall()[0][0])
