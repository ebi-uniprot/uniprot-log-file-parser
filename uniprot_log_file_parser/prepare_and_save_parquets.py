import os.path
import argparse
import sys
import re
import csv
from glob import glob
from collections import defaultdict
from pathlib import Path
import pandas as pd
from duckdb import DuckDBPyConnection
from uniprot_log_file_parser.ua import get_browser_family
from uniprot_log_file_parser.db import (
    get_db_connection,
    is_log_already_saved_as_parquets,
)


def parse_log_line(line: str):
    pattern = re.compile(
        r"^\S+ \S+ \S+ \[(?P<datetime>[^\]]+)\] \""
        r"(?P<method>(GET|HEAD|POST|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH|)) "
        r"(?P<request>[^\"]+)? HTTP/[0-9.]+\" (?P<status>[0-9]{3}) "
        r"(?P<bytes>[0-9]+|-) \"(?P<referrer>[^\"]*)\" \"(?P<useragent>.*)\""
    )
    match = pattern.match(line.strip())
    if match:
        return match.groupdict()


def remove_non_utf8(s: pd.Series):
    return s.str.encode("utf-8", errors="replace").str.decode("utf-8")


def get_log_data_frame(log_path):
    with open(log_path, encoding="utf-8") as file:
        log_contents = file.read()
    data = []
    n_lines_skipped = 0
    for line in log_contents.splitlines():
        parsed = parse_log_line(line)
        if parsed:
            data.append(parsed)
        else:
            print(log_path, "Could not parse:", line, flush=True, file=sys.stderr)
            n_lines_skipped += 1
    df_log = pd.DataFrame(data)
    df_log["request"] = remove_non_utf8(df_log["request"])
    df_log["useragent"] = remove_non_utf8(df_log["useragent"])
    df_log["referrer"] = remove_non_utf8(df_log["referrer"])
    df_log["useragent_family"] = df_log["useragent"].apply(get_browser_family)
    df_log["status"] = df_log["status"].astype("category")
    df_log["method"] = df_log["method"].astype("category")
    df_log.loc[df_log["bytes"] == "-", "bytes"] = "0"
    df_log["bytes"] = pd.to_numeric(df_log["bytes"])
    df_log["datetime"] = pd.to_datetime(
        df_log["datetime"], format="%d/%b/%Y:%H:%M:%S %z"
    )
    df_log = df_log.set_index("datetime")
    return df_log, n_lines_skipped


def save_parquets_by_date(df_log, out_dir):
    for timestamp, df_timestamp in df_log.groupby(pd.Grouper(freq="M")):
        yyyy_mm = timestamp.strftime("%Y-%m")
        filename = f"{yyyy_mm}.parquet"
        filepath = os.path.join(out_dir, filename)
        if os.path.isfile(filepath):
            df_existing = pd.read_parquet(filepath)
            df_timestamp = pd.concat([df_existing, df_timestamp])
        df_timestamp.to_parquet(filepath, index=False)


def get_status_counts(df_log):
    status_counts = defaultdict(int)
    for status, count in df_log.status.value_counts().items():
        k = f"status_{str(status)[0]}xx"
        status_counts[k] += count
    return status_counts


def save_log_meta(
    meta_path: str,
    namespace: str,
    log_path: str,
    total_bytes: int,
    lines_imported: int,
    lines_skipped: int,
    status_counts: defaultdict,
):
    columns = [
        "namespace",
        "log_path",
        "total_bytes",
        "lines_imported",
        "lines_skipped",
        "status_1xx",
        "status_2xx",
        "status_3xx",
        "status_4xx",
        "status_5xx",
    ]
    if not os.path.isfile(meta_path):
        with open(meta_path, "w") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
    with open(meta_path, "a") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                namespace,
                log_path,
                total_bytes,
                lines_imported,
                lines_skipped,
            ]
            + [status_counts[f"status_{s}xx"] for s in range(1, 6)]
        )


def parse_and_save_log_as_parquets(
    out_dir: str, namespace: str, dbc: DuckDBPyConnection, meta_path: str, log_path: str
):
    out_dir_namespace = os.path.join(out_dir, namespace)
    Path(out_dir_namespace).mkdir(parents=True, exist_ok=True)
    log_path = os.path.abspath(log_path)
    if is_log_already_saved_as_parquets(dbc, meta_path, log_path):
        print(f"{log_path} imported already", flush=True, file=sys.stderr)
        return
    df_log, n_lines_skipped = get_log_data_frame(log_path)
    n_lines_parsed = len(df_log)
    print(f"\tAttempting to save {n_lines_parsed} rows")
    save_parquets_by_date(df_log, out_dir_namespace)
    status_counts = get_status_counts(df_log)
    total_bytes = sum(df_log["bytes"])
    save_log_meta(
        meta_path,
        namespace,
        log_path,
        total_bytes,
        n_lines_parsed,
        n_lines_skipped,
        status_counts,
    )


def is_log_in_date_range(log_path: str, start_date: str, end_date: str):
    log_pattern = re.compile(r"\.([0-9\-]+)\.log$")
    match = log_pattern.search(log_path)
    assert match
    log_date = match.groups()[0]
    return start_date <= log_date < end_date


def get_log_paths(log_glob: str, start_date: str, end_date: str):
    log_paths = glob(log_glob)
    if start_date and end_date:
        return [
            path
            for path in log_paths
            if is_log_in_date_range(path, start_date, end_date)
        ]
    return log_paths


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out_dir",
        type=str,
        help="",
    )
    parser.add_argument(
        "--namespace", type=str, help="The namespace of the log file eg uniprotkb"
    )
    parser.add_argument(
        "--log_glob",
        type=str,
        help="A glob pattern for all of the log files to be parsed",
    )
    parser.add_argument(
        "--start_date",
        type=str,
        help="The date from which log files will be included eg 2022-07-01",
        nargs="?",
        const=None,
    )
    parser.add_argument(
        "--end_date",
        type=str,
        help="The date upto which log files will be included eg 2023-06-01",
        nargs="?",
        const=None,
    )
    args = parser.parse_args()
    return args.out_dir, args.namespace, args.log_glob, args.start_date, args.end_date


def main():
    out_dir, namespace, log_glob, start_date, end_date = get_arguments()
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    meta_path = os.path.join(out_dir, "meta.csv")
    dbc = get_db_connection()
    paths = get_log_paths(log_glob, start_date, end_date)
    for index, path in enumerate(paths, 1):
        print(f"{index}/{len(paths)}: {path}")
        try:
            parse_and_save_log_as_parquets(out_dir, namespace, dbc, meta_path, path)
        except Exception as e:
            print(
                "Could not save due to exception:\n",
                path,
                "\n",
                e,
                flush=True,
                file=sys.stderr,
            )
        print("-" * 25)
    dbc.close()


if __name__ == "__main__":
    main()
