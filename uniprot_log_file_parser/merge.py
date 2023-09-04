#!/usr/bin/env python3
import re
from glob import glob
import duckdb


def setup_duckdb():
    duckdb.sql("SET threads TO 4; SET memory_limit='8GB'")


def merge_parquets(date):
    # duckdb.sql(
    print(
        f"COPY (SELECT * FROM '{date}.*.parquet') TO "
        f"'{date}.parquet' (FORMAT 'parquet')"
    )


def get_date(filename):
    p = re.compile(r"^(?P<yyyy_mm>\d{4}-\d{2})\..+\.parquet$")
    m = p.match(filename)
    if m:
        return m.group("yyyy_mm")


def main():
    setup_duckdb()
    dates = {get_date(f) for f in glob("*")}
    for yyyy_mm in dates:
        if yyyy_mm:
            merge_parquets(yyyy_mm)


if __name__ == "__main__":
    main()
