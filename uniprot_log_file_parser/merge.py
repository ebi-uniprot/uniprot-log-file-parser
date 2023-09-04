#!/usr/bin/env python3
import argparse
from collections import defaultdict
import os
import re
from glob import glob
import duckdb


def get_duckdb_connection():
    duckdb_con = duckdb.connect()
    duckdb_con.sql("SET threads TO 8")
    duckdb_con.sql("SET memory_limit='64GB'")
    return duckdb_con


def get_chunks_counts(duckdb_con, date):
    pass


def get_chunks(date):
    filenames = os.listdir()
    date_to_paths = defaultdict(list)
    for filename in filenames:
        date = get_date(filename)
        date_to_paths[date] += filename
    return date_to_paths


def get_main_filename(date):
    return f"{date}.parquet"


def does_main_exist(filename):
    return os.path.exists(filename)


def get_parquets_list(parquets):
    return [f"'{p}'" for p in parquets].join(",")


def merge_parquets(duckdb_con, from_parquets_list, to_parquet):
    duckdb_con.sql(
        f"COPY (SELECT * FROM [{from_parquets_list}]"
        f" TO '{to_parquet}' (FORMAT 'parquet')"
    )


def count_parquets_list(duckdb_con, parquets_list):
    duckdb_con.sql(f"COUNT (SELECT * FROM [{parquets_list}])")


def get_date(filename):
    p = re.compile(r"^(?P<yyyy_mm>\d{4}-\d{2})\..+\.parquet$")
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
            # get_chunks_counts()
            # get_main_counts()
            merge_parquets(duckdb_con, yyyy_mm)
            # check_counts()
            # remove_chunks()


if __name__ == "__main__":
    main()
