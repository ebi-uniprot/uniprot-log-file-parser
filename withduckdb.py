#!/usr/bin/env python3
import sys
import re
from glob import glob
import duckdb
import pandas as pd
import hashlib

namespace = "idmapping"
start_date = "2022-07-01"
end_date = "2024-07-01"
pattern = f"/Volumes/Developer/logs/{namespace}/*/*.log"


def parse_log_entry(entry):
    p = re.compile(
        r"^\S+ \S+ \S+ \[(?P<datetime>[^\]]+)\] \"(?P<method>[A-Z]+) (?P<request>[^ \"]+)? HTTP/[0-9.]+\" (?P<status>[0-9]{3}) (?P<bytes>[0-9]+|-) \"(?P<referrer>[^\"]*)\" \"(?P<useragent>.*)\""
    )
    m = p.match(entry)
    if m:
        return m.groupdict()


def is_log_inserted_already(sha256hash, db_connection):
    log_imported_already = db_connection.execute(
        f"SELECT COUNT(*) FROM insertedlogs WHERE sha256hash = '{sha256hash}'"
    )
    return log_imported_already.fetchall()[0][0]


def get_hash(log_contents):
    return hashlib.sha256(log_contents.encode("utf-8")).hexdigest()


def prepare_and_insert_log(log_path, db_connection):
    with open(log_path) as f:
        log_contents = f.read()
    sha256hash = get_hash(log_contents)
    if is_log_inserted_already(sha256hash, db_connection):
        print(f"{log_path} imported already", flush=True, file=sys.stderr)
        return
    data = []
    n_lines_skipped = 0
    for line in log_contents.splitlines():
        parsed = parse_log_entry(line.strip())
        if parsed:
            data.append(parsed)
        else:
            print(log_path, "Could not parse:", line, flush=True, file=sys.stderr)
            n_lines_skipped += 1
    df = pd.DataFrame(data)
    df["status"] = pd.to_numeric(df["status"])
    df.loc[df["bytes"] == "-", "bytes"] = "0"
    df["bytes"] = pd.to_numeric(df["bytes"])
    df["datetime"] = pd.to_datetime(df["datetime"], format="%d/%b/%Y:%H:%M:%S %z")
    n_lines_imported = len(df)
    print(f"Attempting to add {n_lines_imported} rows from {log_path}")
    db_connection.execute(f"INSERT INTO {namespace} SELECT * FROM df")
    db_connection.execute(
        f"INSERT INTO insertedlogs VALUES ('{sha256hash}', {n_lines_imported}, {n_lines_skipped})"
    )


def is_log_in_date_range(path):
    p = re.compile(r"\.([0-9\-]+)\.log$")
    m = p.search(path)
    assert m
    date = m.groups()[0]
    return start_date <= date <= end_date


def main():
    db_connection = duckdb.connect(database="my-db.duckdb", read_only=False)
    db_connection.execute(
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
    db_connection.execute(
        """
        CREATE TABLE IF NOT EXISTS
            insertedlogs(
                sha256hash VARCHAR PRIMARY KEY,
                n_lines_imported BIGINT,
                n_lines_skipped BIGINT
            )
        """
    )
    paths = [path for path in glob(pattern) if is_log_in_date_range(path)]
    for index, path in enumerate(paths):
        print(f"{index+1}/{len(paths)}")
        prepare_and_insert_log(path, db_connection)


if __name__ == "__main__":
    main()
