#!/usr/bin/env python3
import argparse
import sys
import re
from glob import glob
import hashlib
import pandas as pd
from uniprot_log_file_parser.db import (
    get_db_connection,
    get_useragent_df,
    insert_log_data,
    insert_log_meta,
    is_log_already_inserted,
    setup_tables,
)

START_DATE = "2022-07-01"


def parse_log_line(line: str):
    pattern = re.compile(
        r"^\S+ \S+ \S+ \[(?P<datetime>[^\]]+)\] \"(?P<method>[A-Z]+) "
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


def merge_useragents(dbc, log_df):
    useragents = get_useragent_df(dbc)


def parse_and_insert_log_file(namespace, dbc, log_path):
    with open(log_path, encoding="utf-8") as file:
        log_contents = file.read()
        sha512_hash = get_sha512_hash(log_contents)
        if is_log_already_inserted(dbc, sha512_hash):
            print(f"{log_path} imported already", flush=True, file=sys.stderr)
            return
    log_df, n_lines_skipped = get_log_data_frame(log_contents, log_path)
    n_lines_imported = len(log_df)
    print(f"Attempting to add {n_lines_imported} rows from {log_path}")
    insert_log_data(dbc, namespace, log_df)
    insert_log_meta(dbc, sha512_hash, n_lines_imported, n_lines_skipped)


def is_log_in_date_range(log_path: str):
    log_pattern = re.compile(r"\.([0-9\-]+)\.log$")
    match = log_pattern.search(log_path)
    assert match
    date = match.groups()[0]
    return START_DATE <= date


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "namespace", type=str, help="The namespace of the log file eg uniprotkb"
    )
    parser.add_argument(
        "log_glob",
        type=str,
        help="A glob pattern for all of the log files to be parsed",
    )
    parser.add_argument(
        "db_path",
        type=str,
        help="The path to the Duckdbc file to hold the parsed log data",
    )
    args = parser.parse_args()
    return args.namepsace, args.log_glob, args.db_path


def get_log_paths(log_glob: str):
    return [path for path in glob(log_glob) if is_log_in_date_range(path)]


def main():
    namespace, log_glob, db_path = get_arguments()
    dbc = get_db_connection(db_path)
    setup_tables(dbc, namespace)

    paths = get_log_paths(log_glob)
    for index, path in enumerate(paths, 1):
        print(f"{index}/{len(paths)}")

        prepare_and_insert_log_file(namespace, dbc, path)


if __name__ == "__main__":
    main()
