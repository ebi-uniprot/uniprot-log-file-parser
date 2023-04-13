from duckdb import connect, DuckDBPyConnection, ParserException
from pandas import DataFrame, read_csv


def get_db_connection(db_path: str):
    return connect(database=db_path, read_only=False)


def setup_tables(dbc: DuckDBPyConnection, namespace: str):
    dbc.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {namespace}(
            datetime TIMESTAMP WITH TIME ZONE,
            method VARCHAR,
            request VARCHAR,
            status USMALLINT,
            bytes UBIGINT,
            referrer VARCHAR,
            useragent_id INTEGER
        )
        """
    )
    dbc.sql(
        """
        CREATE TABLE IF NOT EXISTS
            insertedlogs(
                sha512hash VARCHAR PRIMARY KEY,
                n_lines_imported UINTEGER,
                n_lines_skipped UINTEGER
            )
        """
    )
    try:
        dbc.sql(
            """
            CREATE TYPE IF NOT EXISTS useragent_type AS ENUM(
                'browser',
                'programmatic',
                'bot', 'unknown'
            );
            """
        )
    except ParserException:
        # useragent_type exists
        pass
    dbc.sql(
        """
        CREATE TABLE IF NOT EXISTS useragent(
            string VARCHAR UNIQUE NOT NULL,
            family VARCHAR NOT NULL,
            type useragent_type,
            major BOOLEAN,
            id INTEGER PRIMARY KEY NOT NULL,
        )
        """
    )


def insert_log_data(
    dbc: DuckDBPyConnection,
    namespace: str,
    log_df: DataFrame  # pylint: disable=unused-argument
):
    dbc.sql(f"INSERT INTO {namespace} SELECT * FROM log_df")


def insert_log_meta(
    dbc: DuckDBPyConnection,
    sha512hash: str,
    n_lines_imported: int,
    n_lines_skipped: int
):
    dbc.sql(
        "INSERT INTO logmeta VALUES "
        f"('{sha512hash}', {n_lines_imported}, {n_lines_skipped})"
    )


def is_log_already_inserted(dbc: DuckDBPyConnection, sha512hash: str):
    results = dbc.sql(
        f"SELECT COUNT(*) FROM insertedlogs WHERE sha512hash = '{sha512hash}'"
    )
    return bool(results.fetchall()[0][0])


def get_useragents(dbc: DuckDBPyConnection):
    return dbc.sql("SELECT * FROM useragent").to_df()


def update_useragents(
    dbc: DuckDBPyConnection,
    useragents: DataFrame  # pylint: disable=unused-argument
):
    dbc.sql("INSERT OR IGNORE INTO useragent SELECT * FROM useragents")


def restore_useragent(dbc: DuckDBPyConnection, csv_file: str):
    uadf = read_csv(csv_file)
    uadf['id'] = uadf.index
    uadf['string'] = uadf['string'].fillna("")
    uadf["type"] = uadf["type"].astype("category")
    update_useragents(dbc, uadf)
