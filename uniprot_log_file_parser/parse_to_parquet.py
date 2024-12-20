#!/usr/bin/env python3
import os.path
import argparse
import re
from collections import defaultdict
from pathlib import Path
import pandas as pd
from uniprot_log_file_parser.ua import get_browser_family
from uniprot_log_file_parser.meta import (
    encode_log_path,
    in_meta,
    save_meta,
    save_meta_columns,
)


def parse_log_line(line: str) -> dict[str, str]:
    pattern = re.compile(
        r"^\S+ \S+ \S+ \[(?P<datetime>[^\]]+)\] \""
        r"(?P<method>(GET|HEAD|POST|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH|)) "
        r"(?P<request>[^\"]+)? HTTP/[0-9.]+\" (?P<status>[0-9]{3}) "
        r"(?P<bytes>[0-9]+|-) \"(?P<referrer>[^\"]*)\" \"(?P<useragent>.*)\""
    )
    match = pattern.match(line.strip())
    if match:
        return match.groupdict()


def is_static(request: str) -> bool:
    pattern = re.compile(
        r"(/(images|scripts|style))|(\.(ico|css|png|jpg|svg|js|woff))|opensearch.xml"
    )
    return bool(pattern.search(request))


def get_log_data_frame(log_path: str, is_legacy=False) -> pd.DataFrame:
    try:
        with open(log_path, encoding="utf-8") as file:
            log_contents = file.read()
    except UnicodeDecodeError:
        # Legacy logs sometime have non utf-8 characters
        with open(log_path, encoding="ISO-8859-1") as file:
            log_contents = file.read().encode("utf-8", errors="replace").decode("utf-8")

    data = []
    n_lines_skipped = 0
    for line in log_contents.splitlines():
        parsed = parse_log_line(line)
        if parsed:
            if is_legacy and is_static(parsed["request"]):
                continue
            data.append(parsed)
        else:
            print(log_path, "Could not parse:", line)
            n_lines_skipped += 1
    df_log = pd.DataFrame(data)
    df_log["useragent_family"] = df_log["useragent"].apply(get_browser_family)
    df_log["status"] = df_log["status"].astype("category")
    df_log["method"] = df_log["method"].astype("category")
    df_log.loc[df_log["bytes"] == "-", "bytes"] = "0"
    df_log["bytes"] = pd.to_numeric(df_log["bytes"])
    df_log["datetime"] = pd.to_datetime(
        df_log["datetime"], format="%d/%b/%Y:%H:%M:%S %z", errors="coerce"
    )
    n_lines_skipped += df_log["datetime"].isna().sum()
    df_log = df_log.dropna(how="all", subset="datetime")
    df_log = df_log.set_index("datetime")
    return df_log, n_lines_skipped


def save_parquets_by_date(df_log: pd.DataFrame, out_dir: str, log_path: str):
    encoded_log_path = encode_log_path(log_path)
    for timestamp, df_timestamp in df_log.groupby(pd.Grouper(freq="M")):
        yyyy_mm = timestamp.strftime("%Y-%m")
        filename = f"{yyyy_mm}.{encoded_log_path}.parquet"
        filepath = os.path.join(out_dir, filename)
        if os.path.isfile(filepath):
            raise FileExistsError(filepath)
        df_timestamp.to_parquet(filepath)


def get_status_counts(df_log: pd.DataFrame) -> defaultdict(int):
    status_counts = defaultdict(int)
    for status, count in df_log.status.value_counts().items():
        k = f"status_{str(status)[0]}xx"
        status_counts[k] += count
    return status_counts


def parse_and_save_log_as_parquets(
    out_dir: str, meta_dir: str, log_path: str, is_legacy=False
) -> None:
    out_dir_parquet = os.path.join(out_dir, "parquet")
    Path(out_dir_parquet).mkdir(parents=True, exist_ok=True)
    log_path = os.path.abspath(log_path)
    if in_meta(meta_dir, log_path):
        print(f"{log_path} saved already")
        return
    df_log, n_lines_skipped = get_log_data_frame(log_path, is_legacy)
    n_lines_parsed = len(df_log)
    print(f"Saving {n_lines_parsed} rows")
    save_parquets_by_date(df_log, out_dir_parquet, log_path)
    status_counts = get_status_counts(df_log)
    total_bytes = sum(df_log["bytes"])
    save_meta(
        meta_dir,
        log_path,
        total_bytes,
        n_lines_parsed,
        n_lines_skipped,
        status_counts,
    )


def get_arguments() -> [str, str, bool]:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out_dir",
        type=str,
        help="The directory where the parquets, job details and meta will be stored",
    )
    parser.add_argument(
        "--log_path",
        type=str,
        help="The path to the log file to parse",
    )
    parser.add_argument(
        "--legacy",
        action="store_true",
        default=False,
        help="Flag to indicate if the log is from a legacy server (pre 2022-07)",
    )
    args = parser.parse_args()
    return args.out_dir, args.log_path, args.legacy


def main() -> None:
    out_dir, log_path, is_legacy = get_arguments()
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    meta_dir = os.path.join(out_dir, "meta")
    Path(meta_dir).mkdir(exist_ok=True)
    save_meta_columns(meta_dir)
    parse_and_save_log_as_parquets(out_dir, meta_dir, log_path, is_legacy)


if __name__ == "__main__":
    main()
