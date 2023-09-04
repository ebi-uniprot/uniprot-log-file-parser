#!/usr/bin/env python3
import argparse
from collections import defaultdict
import os
import re
import duckdb


def get_duckdb_connection():
    duckdb_con = duckdb.connect()
    duckdb_con.sql("SET threads TO 8")
    duckdb_con.sql("SET memory_limit='64GB'")
    return duckdb_con


def get_chunk_date(filename):
    p = re.compile(r"^(?P<yyyy_mm>\d{4}-\d{2})\..+\.parquet$")
    m = p.match(filename)
    if m:
        return m.group("yyyy_mm")


def get_chunks():
    filenames = os.listdir()
    date_to_chunk_parquets = defaultdict(list)
    for filename in filenames:
        yyyy_mm = get_chunk_date(filename)
        if yyyy_mm:
            date_to_chunk_parquets[yyyy_mm].append(filename)
    return date_to_chunk_parquets


def get_main_filename(date):
    return f"{date}.parquet"


def get_parquets_list(parquets):
    return ",".join([f"'{p}'" for p in parquets])


def merge_parquets(duckdb_con, from_parquets, to_parquet):
    from_parquets = get_parquets_list(from_parquets)
    duckdb_con.sql(
        f"COPY (SELECT * FROM [{from_parquets}]"
        f" TO '{to_parquet}' (FORMAT 'parquet')"
    )


def get_parquets_count(duckdb_con, parquets):
    from_parquets = get_parquets_list(parquets)
    return duckdb_con.sql(f"SELECT COUNT(*) FROM read_parquet([{from_parquets}])")


def remove_parquets(parquets: list[str]) -> None:
    for parquet in parquets:
        assert parquet.endswith(".parquet")
        os.remove(parquet)


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
    chunks = get_chunks()
    for yyyy_mm, chunk_parquets in chunks.items():
        print("-" * 20)
        print(f"Merging parquets from {yyyy_mm}:")
        print("\n".join([f"\t{p}" for p in chunk_parquets]))
        main_parquet = get_main_filename(yyyy_mm)
        if os.path.exists(main_parquet):
            from_parquets = chunk_parquets + [main_parquet]
        else:
            from_parquets = chunk_parquets
        from_parquets_count = get_parquets_count(duckdb_con, from_parquets)
        merge_parquets(duckdb_con, from_parquets, main_parquet)
        merged_count = get_parquets_count(duckdb_con, [main_parquet])
        assert merged_count == from_parquets_count
        remove_parquets(chunk_parquets)


if __name__ == "__main__":
    main()
