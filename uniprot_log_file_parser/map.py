#!/usr/bin/env python3
import argparse
from collections import defaultdict
import sys
import os.path
from pathlib import Path
import csv
import pandas as pd

from .log_entry import LogEntry
from .utils import get_YYYY_MM_DD_from_filename, get_out_filename


def parse_log_file(log_file_path, out_directory):

    tally_bytes = defaultdict(int)
    lines_to_write = []
    with open(log_file_path, "r", encoding="ISO-8859-1") as f:
        line_number = 0
        while True:
            to_write = {}
            line_number += 1
            try:
                line = f.readline()
            except Exception as e:
                print(
                    f"ReadlineError [line {line_number}]: {e}",
                    flush=True,
                    file=sys.stderr,
                )
                continue
            if not line:
                break
            try:
                line = line.replace("%0D", "")
                entry = LogEntry(line)
            except Exception as e:
                print(e, flush=True, file=sys.stderr)
                continue

            # Some requests were made in 1969-12-31 for some reason
            if entry.is_request_unreasonably_old():
                continue

            # Daily data traffic
            date_string = entry.get_date_string()
            size_bytes = entry.get_bytes()
            if size_bytes:
                tally_bytes[date_string] += size_bytes

            if entry.is_static():
                continue

            # We are only interested in queries made to a specific domain (eg uniprotkb)
            # and not requests to resources such as /scripts, /style
            if not entry.has_valid_namespace():
                continue

            try:
                timestamp = entry.get_timestamp()
                method, resource = entry.get_method_resource()
                status = entry.get_response_code()
                if not all([timestamp, method, resource, status]):
                    continue
                to_write["DateTime"] = timestamp
                to_write["Method"] = method
                to_write["Resource"] = resource
                to_write["Namespace"] = entry.get_namespace()
                to_write["Status"] = status
                to_write["SizeBytes"] = size_bytes
                to_write["ResponseTime"] = entry.get_response_time()
                to_write["Referer"] = entry.get_referer()
                to_write["UserAgentFamily"] = entry.get_user_agent_browser_family()
                to_write["IP"] = entry.get_ip()
                lines_to_write.append(to_write)

            except Exception as e:
                print(e, log_file_path, line, flush=True, file=sys.stderr)

    log_date = get_YYYY_MM_DD_from_filename(log_file_path)
    parsed_filename = get_out_filename(log_file_path, "parsed", "csv")
    parsed_directory = os.path.join(out_directory, "parsed", log_date)
    Path(parsed_directory).mkdir(parents=True, exist_ok=True)
    parsed_filepath = os.path.join(parsed_directory, parsed_filename)
    print(parsed_filepath)
    df = pd.DataFrame(lines_to_write)
    df.to_csv(parsed_filepath, header=False, index=False)

    tally_filename = get_out_filename(log_file_path, "daily-bytes", "csv")
    tally_directory = os.path.join(out_directory, "tally")
    Path(tally_directory).mkdir(parents=True, exist_ok=True)
    tally_filepath = os.path.join(tally_directory, tally_filename)
    with open(tally_filepath, "w") as f:
        writer = csv.writer(f)
        for k, v in tally_bytes.items():
            writer.writerow([k, v])


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "log_file_path", type=str, help="The path to log file to be parsed"
    )
    parser.add_argument(
        "out_directory",
        type=str,
        help="The path to the directory for the resulting csv file to be saved",
    )
    args = parser.parse_args()
    return args.log_file_path, args.out_directory


def main():
    log_file_path, out_directory = get_arguments()
    print(f"Parsing: {log_file_path} and saving output to directory: {out_directory}")
    parse_log_file(log_file_path, out_directory)


if __name__ == "__main__":
    main()
