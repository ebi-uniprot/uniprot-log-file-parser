#!/usr/bin/env python3
import argparse
from collections import defaultdict
import sys

from .log_entry import LogEntry
from .utils import write_counts_to_csv, write_parsed_lines_to_csv

FIELDNAMES = [
    'date',
    'api',
    'resource',
    'referer',
]


def parse_log_file(log_file_path):
    tally_n_requests = defaultdict(int)
    tally_bytes = defaultdict(int)
    lines_to_write = []
    with open(log_file_path, 'r', encoding='ISO-8859-1') as f:
        line_number = 0
        while True:
            to_write = {}
            line_number += 1
            try:
                line = f.readline()
            except Exception as e:
                print(f'ReadlineError [line {line_number}]: {e}',
                      flush=True, file=sys.stderr)
                continue
            if not line:
                break
            try:
                entry = LogEntry(line)
            except Exception as e:
                print(e, flush=True, file=sys.stderr)
                continue
            if not entry.is_success():
                continue
            if entry.is_bot():
                continue
            # Some requests were made in 1969-12-31 for some reason
            if entry.is_request_unreasonably_old():
                continue

            # Daily data traffic
            yyyy_mm_dd = entry.get_yyyy_mm_dd()
            tally_n_requests[yyyy_mm_dd] += 1

            b = entry.get_bytes()
            if b:
                tally_bytes[yyyy_mm_dd] += b

            if not entry.is_get():
                continue

            # This check is already covered by is_bot check above
            # if entry.is_unknown_agent():
            #     continue

            # We don't need to count a query multiple times if facets are being used
            if entry.query_has_facets():
                continue

            if entry.is_opensearch():
                continue
            # We are only interested in queries made to a specific domain (eg uniprotkb)
            # and not requests to resources such as /scripts, /style
            if not entry.has_valid_namespace():
                continue
            to_write['date'] = yyyy_mm_dd
            to_write['api'] = int(entry.is_api())
            try:
                # Parse resource
                resource = entry.get_resource()
                to_write['resource'] = resource
                # to_write['namespace'] = entry.get_uniprot_namespace(resource)
                to_write['referer'] = entry.get_referer()

            except Exception as e:
                print(e, log_file_path, line, flush=True, file=sys.stderr)

            lines_to_write.append(to_write)

    return tally_n_requests, tally_bytes, lines_to_write


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
    tally_n_requests, tally_bytes, parsed = parse_log_file(
        log_file_path)
    if tally_n_requests:
        write_counts_to_csv(out_directory, log_file_path,
                            'n-requests', tally_n_requests)
    if tally_bytes:
        write_counts_to_csv(out_directory, log_file_path, 'bytes', tally_bytes)
    if parsed:
        write_parsed_lines_to_csv(
            out_directory, log_file_path, 'parsed', parsed, FIELDNAMES)


if __name__ == '__main__':
    main()
