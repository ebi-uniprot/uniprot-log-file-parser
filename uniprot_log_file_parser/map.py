#!/usr/bin/env python3
import argparse
from collections import defaultdict
import sys
import os.path
import csv
import json

from .log_entry import LogEntry
from .utils import get_out_filename, write_counts_to_csv, write_parsed_lines


def parse_log_file(log_file_path, out_directory):

    method_to_id = {
        "GET": 1,
        "HEAD": 2,
        "POST": 3,
        "PUT": 4,
        "DELETE": 5,
        "CONNECT": 6,
        "OPTIONS": 7,
        "TRACE": 8,
        "PATCH": 9,
    }

    tally_n_requests = defaultdict(int)
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
                entry = LogEntry(line)
            except Exception as e:
                print(e, flush=True, file=sys.stderr)
                continue

            # Some requests were made in 1969-12-31 for some reason
            if entry.is_request_unreasonably_old():
                continue

            if entry.is_static():
                continue

            # We are only interested in queries made to a specific domain (eg uniprotkb)
            # and not requests to resources such as /scripts, /style
            if not entry.has_valid_namespace():
                continue

            # Daily data traffic
            date_string = entry.get_date_string()
            tally_n_requests[date_string] += 1

            try:
                timestamp = entry.get_timestamp()
                method, resource = entry.get_method_resource()
                status = entry.get_response_code()
                if not all(
                    [timestamp, method, resource, status, method in method_to_id]
                ):
                    continue
                to_write["DateTime"] = timestamp
                to_write["MethodId"] = method_to_id[method]
                to_write["Resource"] = resource
                to_write["Status"] = status
                to_write["SizeBytes"] = entry.get_bytes()
                to_write["ResponseTime"] = entry.get_response_time()
                to_write["Referer"] = entry.get_referer()
                to_write["UserAgent"] = entry.get_user_agent()
                lines_to_write.append(to_write)

            except Exception as e:
                print(e, log_file_path, line, flush=True, file=sys.stderr)

    parsed_filename = get_out_filename(log_file_path, "parsed", "json")
    parsed_filepath = os.path.join(out_directory, parsed_filename)
    with open(parsed_filepath, "w") as f:
        json.dump(lines_to_write, f)

    tally_filename = get_out_filename(log_file_path, "n-requests", "csv")
    tally_filepath = os.path.join(out_directory, tally_filename)
    with open(tally_filepath, "w") as f:
        writer = csv.writer(f)
        for k, v in tally_n_requests.items():
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
