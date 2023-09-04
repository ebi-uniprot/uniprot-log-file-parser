#!/usr/bin/env python3
import argparse
import os
import re
from glob import glob
import duckdb


def setup_duckdb():
    duckdb.sql("SET threads TO 3; SET memory_limit='6GB'")


def merge_parquets(date):
    duckdb.sql(
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
    setup_duckdb()
    os.chdir(working_dir)
    dates = {get_date(f) for f in glob("*")}
    for yyyy_mm in dates:
        if yyyy_mm:
            merge_parquets(yyyy_mm)


if __name__ == "__main__":
    main()
