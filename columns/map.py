#!/usr/bin/env python3
import urllib.parse
import re
import json
from collections import Counter
import argparse
import os
import sys


def read_json_file(json_file):
    with open(json_file) as f:
        return json.load(f)


def write_json_file(obj, file_path):
    with open(file_path, 'w') as f:
        json.dump(obj, f, indent=4)


def get_out_file_path(log_json_file, output_directory):
    filename = os.path.splitext(os.path.basename(log_json_file))[0]
    return os.path.join(output_directory, f'{filename}.number-column-counts.json')


def get_params(url):
    params = urllib.parse.urlparse(url)
    return urllib.parse.parse_qs(params.query)


def get_url_from_resource(resource):
    p = re.compile(r'GET\s(?P<url>.*)\sHTTP/.*',
                   re.IGNORECASE | re.DOTALL)
    m = p.match(resource)
    return m.group('url')


def get_number_columns(params):
    if 'columns' in params:
        return len(params['columns'][0].split(','))
    else:
        return 0


def get_number_column_counts_from_log_json_file(log_json_file):
    log = read_json_file(log_json_file)
    number_columns = []
    for entry in log:
        resource = entry['resource']
        try:
            url = get_url_from_resource(resource)
            resource_params = get_params(url)
            resource_number_columns = get_number_columns(resource_params)
            number_columns.append(resource_number_columns)
        except Exception as e:
            print(f'Exception {e} occured with {resource}.',
                  flush=True, file=sys.stderr)
    return Counter(number_columns)


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('log_json_file', type=str,
                        help='The log json file to count term stats')
    parser.add_argument('output_directory', type=str,
                        help='The directory where the resulting counts file will be saved')
    args = parser.parse_args()
    return args.log_json_file, args.output_directory


def main():
    log_json_file, output_directory = get_arguments()
    print(f'log_json_file={log_json_file}', flush=True)
    print(f'output_directory={output_directory}', flush=True)
    number_columns_counts = get_number_column_counts_from_log_json_file(
        log_json_file)
    out_file_path = get_out_file_path(log_json_file, output_directory)
    print(
        f'writing {len(number_columns_counts)} counts to {out_file_path}', flush=True)
    write_json_file(number_columns_counts, out_file_path)


if __name__ == '__main__':
    main()
