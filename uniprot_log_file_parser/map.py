#!/usr/bin/env python3
import re
from urllib import parse, request
from os import path
import argparse
from collections import defaultdict
from datetime import datetime
import sys

from .log_entry import LogEntry
from .lucene_query import get_field_to_value_counts_from_query
from .utils import merge_list_defaultdicts, write_counts_to_csv, write_parsed_lines_to_csv

FIELDNAMES = [
    'domain',
    'user-type',
    'user-agent-family',
    'query'
]


def parse_log_file(log_file_path):
    tally_n_requests = defaultdict(int)
    tally_bytes = defaultdict(int)
    tally_user_type = defaultdict(int)
    field_to_values = defaultdict(list)
    lines_to_write = []
    with open(log_file_path, 'r', encoding='unicode_escape') as f:
        line_number = 0
        while True:
            to_write = {}
            line_number += 1
            try:
                line = f.readline()
            except Exception as e:
                print(f'[line {line_number}]: {e}',
                      flush=True, file=sys.stderr)
            if not line:
                break
            entry = LogEntry(line)
            if not entry:
                print(
                    f'Skipping (unable to parse): {line}', flush=True, file=sys.stderr)
            if not entry.is_success():
                continue
            if entry.is_bot():
                tally_user_type['bot'] += 1
                continue

            # Daily data traffic
            yyyy_mm_dd = entry.get_yyyy_mm_dd()
            tally_n_requests[yyyy_mm_dd] += 1

            if not entry.is_get():
                continue

            b = entry.get_bytes()
            if b:
                tally_bytes[yyyy_mm_dd] += b

            if entry.is_unknown_agent():
                tally_user_type['unknown'] += 1
                continue

            tally_user_type['browser'] += 1

            domain = entry.get_domain()
            # We are only interested in queries made to a specific domain (eg uniprotkb)
            # and not requests to resources such as /scripts, /style
            if not domain:
                continue
            to_write['domain'] = domain
            to_write['user-type'] = entry.get_user_type()
            to_write['user-agent-family'] = entry.get_user_agent_family()

            try:
                query = entry.get_query()
                if query:
                    to_write['query'] = query
                    lines_to_write.append(to_write)
                    _field_to_values = get_field_to_value_counts_from_query(
                        query)
                    # print(query, _field_to_values)
                    field_to_values = merge_list_defaultdicts(
                        field_to_values, _field_to_values)
            except Exception as e:
                print(f'Exception {e} occured: {line}',
                      flush=True, file=sys.stderr)

    return tally_n_requests, tally_bytes, tally_user_type, lines_to_write, field_to_values


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('log_file_path', type=str,
                        help='The path to log file to be parsed')
    parser.add_argument('out_directory', type=str,
                        help='The path to the directory for the resulting csv file to be saved')
    args = parser.parse_args()
    return args.log_file_path, args.out_directory


def main():
    log_file_path, out_directory = get_arguments()
    print(
        f'Parsing: {log_file_path} and saving output to directory: {out_directory}')
    tally_n_requests, tally_bytes, tally_user_type, lines_to_write, _ = parse_log_file(
        log_file_path)
    if tally_n_requests:
        write_counts_to_csv(out_directory, log_file_path,
                            'n-requests', tally_n_requests)
    if tally_bytes:
        write_counts_to_csv(out_directory, log_file_path, 'bytes', tally_bytes)
    if tally_user_type:
        write_counts_to_csv(out_directory, log_file_path,
                            'user-type', tally_user_type)
    if lines_to_write:
        write_parsed_lines_to_csv(
            out_directory, log_file_path, 'parsed', lines_to_write, FIELDNAMES)


if __name__ == '__main__':
    main()
