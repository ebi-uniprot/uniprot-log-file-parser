#!/usr/bin/env python3
import argparse
import os
import re
from glob import glob
import duckdb


def get_duckdb_connection():
    duckdb_con = duckdb.connect()
    duckdb_con.sql("SET threads TO 4")
    duckdb_con.sql("SET memory_limit='16GB'")
    return duckdb_con


def merge_parquets(duckdb_con, date):
    duckdb_con.sql(
        f"COPY (SELECT * FROM '{date}.*.parquet') TO "
        f"'{date}.parquet' (FORMAT 'parquet')"
    )


def get_date(filename):
    p = re.compile(r"^(?P<yyyy_mm>\d{4}-\d{2}).*\.parquet$")
    m = p.match(filename)
    if m:
        return m.group("yyyy_mm")


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--working_dir",
        type=str,
        help="The directory where the parquet files will be collected, "
        "merged and saved",
    )
    args = parser.parse_args()
    return args.working_dir


def main():
    working_dir = get_arguments()
    os.chdir(working_dir)
    duckdb_con = get_duckdb_connection()
    dates = {get_date(f) for f in glob("*")}
    for yyyy_mm in dates:
        if yyyy_mm:
            merge_parquets(duckdb_con, yyyy_mm)


if __name__ == "__main__":
    main()
