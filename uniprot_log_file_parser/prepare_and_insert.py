#!/usr/bin/env python3
import os.path
import argparse
import sys
import re
from glob import glob
from collections import defaultdict
from datetime import timedelta, date
import hashlib
import pandas as pd
from uniprot_log_file_parser.db import (
    get_db_connection,
    get_unseen_useragent_df,
    get_unseen_useragent_families,
    insert_log_data,
    insert_log_meta,
    insert_unseen_useragent_families,
    insert_unseen_useragents,
    is_log_already_inserted,
    set_db_memory_limit,
    setup_tables,
)

START_DATE = "2022-07-01"
END_DATE = date.today().strftime("%Y-%m-%d")


def parse_log_line(line: str):
    pattern = re.compile(
        r"^\S+ \S+ \S+ \[(?P<datetime>[^\]]+)\] \""
        r"(?P<method>(GET|HEAD|POST|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH|)) "
        r"(?P<request>[^ \"]+)? HTTP/[0-9.]+\" (?P<status>[0-9]{3}) "
        r"(?P<bytes>[0-9]+|-) \"(?P<referrer>[^\"]*)\" \"(?P<useragent>.*)\""
    )
    match = pattern.match(line.strip())
    if match:
        return match.groupdict()


def get_sha512_hash(log_contents):
    return hashlib.sha512(log_contents.encode("utf-8")).hexdigest()


def get_log_data_frame(log_contents, log_path):
    data = []
    n_lines_skipped = 0
    for line in log_contents.splitlines():
        parsed = parse_log_line(line)
        if parsed:
            data.append(parsed)
        else:
            print(log_path, "Could not parse:", line, flush=True, file=sys.stderr)
            n_lines_skipped += 1
    log_df = pd.DataFrame(data)
    log_df["status"] = pd.to_numeric(log_df["status"])
    log_df.loc[log_df["bytes"] == "-", "bytes"] = "0"
    log_df["bytes"] = pd.to_numeric(log_df["bytes"])
    log_df["datetime"] = pd.to_datetime(
        log_df["datetime"], format="%d/%b/%Y:%H:%M:%S %z"
    )
    return log_df, n_lines_skipped


def get_status_counts(log_df):
    status_counts = defaultdict(int)
    for status, count in log_df.status.value_counts().items():
        k = f"status_{str(status)[0]}xx"
        status_counts[k] += count
    return status_counts


def parse_and_insert_log_file(namespace, dbc, log_path):
    with open(log_path, encoding="utf-8") as file:
        log_contents = file.read()
        sha512_hash = get_sha512_hash(log_contents)
        if is_log_already_inserted(dbc, sha512_hash):
            print(f"{log_path} imported already", flush=True, file=sys.stderr)
            return
    log_df, n_lines_skipped = get_log_data_frame(log_contents, log_path)
    n_lines_parsed = len(log_df)
    print(f"Attempting to add {n_lines_parsed} rows from {log_path}")
    unseen_useragent_df = get_unseen_useragent_df(dbc, log_df)
    unseen_useragent_families = get_unseen_useragent_families(
        dbc, unseen_useragent_df)
    insert_unseen_useragent_families(dbc, unseen_useragent_families)
    insert_unseen_useragents(dbc, unseen_useragent_df)
    status_counts = get_status_counts(log_df)
    insert_log_data(dbc, namespace, log_df)
    log_date = (log_df.iloc[0]["datetime"] + timedelta(hours=6)).date()
    total_bytes = sum(log_df["bytes"])
    abs_log_path = os.path.abspath(log_path)
    insert_log_meta(
        dbc,
        abs_log_path,
        log_date,
        sha512_hash,
        total_bytes,
        n_lines_parsed,
        n_lines_skipped,
        status_counts,
    )


def is_log_in_date_range(log_path: str):
    log_pattern = re.compile(r"\.([0-9\-]+)\.log$")
    match = log_pattern.search(log_path)
    assert match
    log_date = match.groups()[0]
    return START_DATE <= log_date < END_DATE


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--namespace",
        type=str,
        help="The namespace of the log file eg uniprotkb"
    )
    parser.add_argument(
        "--log_glob",
        type=str,
        help="A glob pattern for all of the log files to be parsed",
    )
    parser.add_argument(
        "--db_path",
        type=str,
        help="The path to the Duckdbc file to hold the parsed log data",
    )
    parser.add_argument(
        "--memory_limit",
        type=str,
        nargs="?",
        const="4GB",
        help="Maximum amount of memory reserved for DuckDB. Defaults to 4GB.",
    )
    args = parser.parse_args()
    return args.namespace, args.log_glob, args.db_path, args.memory_limit


def get_log_paths(log_glob: str):
    return [path for path in glob(log_glob) if is_log_in_date_range(path)]


def main():
    namespace, log_glob, db_path, memory_limit = get_arguments()
    dbc = get_db_connection(db_path)
    set_db_memory_limit(dbc, memory_limit)
    setup_tables(dbc, namespace)

    paths = get_log_paths(log_glob)
    for index, path in enumerate(paths, 1):
        print(f"{index}/{len(paths)}")
        parse_and_insert_log_file(namespace, dbc, path)

    dbc.close()


if __name__ == "__main__":
    main()
