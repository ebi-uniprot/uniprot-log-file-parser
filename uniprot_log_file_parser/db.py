import datetime
from collections import defaultdict
from duckdb import connect, DuckDBPyConnection, CatalogException
from pandas import DataFrame, read_csv

from uniprot_log_file_parser.ua import get_browser_family


def no_data(df: DataFrame):
    if not isinstance(df, DataFrame):
        return True
    return df.empty


def get_db_connection(db_path: str):
    return connect(database=db_path, read_only=False)


def setup_tables(dbc: DuckDBPyConnection, namespace: str):
    try:
        dbc.sql(
            """
            CREATE TYPE useragent_family_type AS ENUM(
                'browser',
                'programmatic',
                'bot',
                'unknown'
            );
            CREATE TYPE http_method AS ENUM(
                'GET',
                'HEAD',
                'POST',
                'PUT',
                'DELETE',
                'CONNECT',
                'OPTIONS',
                'TRACE',
                'PATCH'
            );
            """
        )
    except CatalogException:
        # useragent_family_type already exists
        pass
    dbc.sql(
        f"""
        CREATE TABLE IF NOT EXISTS useragent_family(
            id INTEGER PRIMARY KEY NOT NULL,
            family VARCHAR UNIQUE NOT NULL,
            type useragent_family_type,
            major BOOLEAN,
            UNIQUE(id, family, type, major)
        );

        CREATE TABLE IF NOT EXISTS useragent(
            id INTEGER PRIMARY KEY NOT NULL,
            string VARCHAR UNIQUE NOT NULL,
            family_id INTEGER NOT NULL,
            FOREIGN KEY(family_id) REFERENCES useragent_family(id),
            UNIQUE(id, string, family_id)
        );
        
        CREATE TABLE IF NOT EXISTS {namespace}(
            datetime TIMESTAMP WITH TIME ZONE,
            method http_method NOT NULL,
            request VARCHAR NOT NULL,
            status USMALLINT NOT NULL,
            bytes UBIGINT NOT NULL,
            referrer VARCHAR,
            useragent_id INTEGER NOT NULL,
            FOREIGN KEY(useragent_id) REFERENCES useragent(id)
        );

        CREATE TABLE IF NOT EXISTS log_meta(
            date DATE NOT NULL,
            sha512hash VARCHAR PRIMARY KEY,
            lines_imported UINTEGER NOT NULL,
            lines_skipped UINTEGER NOT NULL,
            status_1xx UINTEGER NOT NULL,
            status_2xx UINTEGER NOT NULL,
            status_3xx UINTEGER NOT NULL,
            status_4xx UINTEGER NOT NULL,
            status_5xx UINTEGER NOT NULL
        );
        """
    )


def insert_log_data(
    dbc: DuckDBPyConnection,
    namespace: str,
    log_df: DataFrame,
):
    if no_data(log_df):
        return
    dbc.sql(
        f"""
    INSERT INTO
        {namespace}
    SELECT
        datetime,
        method,
        request,
        status,
        bytes,
        referrer,
        useragent.id AS useragent_id
    FROM
        log_df
    LEFT JOIN
        useragent on log_df.useragent = useragent.string
    """
    )


def insert_log_meta(
    dbc: DuckDBPyConnection,
    date: datetime.date,
    sha512hash: str,
    n_lines_imported: int,
    n_lines_skipped: int,
    status_counts: defaultdict
):
    data = [f"'{date}'", f"'{sha512hash}'", n_lines_imported, n_lines_skipped] + \
        [status_counts[f"status_{s}xx"] for s in range(1, 6)]
    dbc.sql(
        f"INSERT INTO log_meta VALUES ({','.join([str(el) for el in data])})"
    )


def is_log_already_inserted(dbc: DuckDBPyConnection, sha512hash: str):
    return bool(dbc.sql(
        f"SELECT sha512hash FROM log_meta WHERE sha512hash = '{sha512hash}'"
    ))


def get_useragent_df(dbc: DuckDBPyConnection):
    return dbc.sql("SELECT * FROM useragent").to_df()


def get_useragent_family_df(dbc: DuckDBPyConnection):
    return dbc.sql("SELECT * FROM useragent_family").to_df()


def update_useragents(dbc: DuckDBPyConnection, useragents_df: DataFrame):
    if no_data(useragents_df):
        return
    dbc.sql("INSERT OR IGNORE INTO useragent SELECT * FROM useragents_df")


def update_useragent_families(
    dbc: DuckDBPyConnection, useragent_families_df: DataFrame
):
    if no_data(useragent_families_df):
        return
    dbc.sql(
        """
        INSERT OR IGNORE INTO
            useragent_family
        SELECT
            *
        FROM
            useragent_families_df
        """
    )


def restore_useragent(dbc: DuckDBPyConnection, csv_file: str):
    ua_df = read_csv(csv_file)
    ua_df["string"] = ua_df["string"].fillna("")
    ua_df["id"] = ua_df.index
    ua_df = ua_df[["id", "string", "family_id"]]
    # uadf["type"] = uadf["type"].astype("category")
    update_useragents(dbc, ua_df)


def restore_useragent_family(dbc: DuckDBPyConnection, csv_file: str):
    uaf_df = read_csv(csv_file)
    update_useragent_families(dbc, uaf_df)


def get_unseen_useragent_df(dbc, log_df):
    if no_data(log_df):
        return
    result = dbc.sql(
        """
    SELECT DISTINCT
        useragent
    FROM
        log_df
    WHERE
        useragent NOT IN (SELECT string FROM useragent)
    """
    ).fetchall()
    if not result:
        return
    unseen_useragents = {el[0] for el in result}
    start_id = dbc.sql("SELECT MAX(id) FROM useragent").fetchone()[0] + 1
    unseen_useragent_items = [
        {
            "string": string,
            "id": i,
        }
        for i, string in enumerate(unseen_useragents, start_id)
    ]
    unseen_useragent_df = DataFrame(unseen_useragent_items)
    unseen_useragent_df["family"] = unseen_useragent_df["string"].apply(
        get_browser_family
    )
    return unseen_useragent_df


def get_unseen_useragent_families(dbc, unseen_useragent_df):
    if no_data(unseen_useragent_df):
        return
    result = dbc.sql(
        """
    SELECT DISTINCT
        family
    FROM
        unseen_useragent_df
    WHERE
        family NOT IN (SELECT family FROM useragent_family)
    """
    ).fetchall()
    if not result:
        return
    return [el[0] for el in result]


def insert_unseen_useragent_families(
        dbc: DuckDBPyConnection,
        unseen_useragent_families: list[str]):
    if not unseen_useragent_families:
        return
    start_id = dbc.sql(
        "SELECT MAX(id) FROM useragent_family").fetchone()[0] + 1
    unseen_useragent_family_items = [
        {
            "id": i,
            "family": string,
        }
        for i, string in enumerate(unseen_useragent_families, start_id)
    ]
    unseen_useragent_family_df = DataFrame(unseen_useragent_family_items)
    unseen_useragent_family_df["type"] = None
    unseen_useragent_family_df["major"] = None
    update_useragent_families(dbc, unseen_useragent_family_df)


def insert_unseen_useragents(
        dbc: DuckDBPyConnection,
        unseen_useragent_df: DataFrame):
    if no_data(unseen_useragent_df):
        return
    useragent_family_df = get_useragent_family_df(dbc)
    useragent_family_df = useragent_family_df.rename(
        columns={"id": "family_id"})
    merged = unseen_useragent_df.merge(useragent_family_df, on="family")
    merged = merged[["id", "string", "family_id"]]
    update_useragents(dbc, merged)
