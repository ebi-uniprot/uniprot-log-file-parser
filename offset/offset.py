#!/usr/bin/env python3
import urllib.parse
import re
import json
from collections import Counter
import argparse
import os


def read_json_file(json_file):
    with open(json_file) as f:
        return json.load(f)


def write_json_file(obj, file_path):
    with open(file_path, 'w') as f:
        json.dump(obj, f, indent=4)


def get_out_file_path(log_json_file, output_directory):
    filename = os.path.splitext(os.path.basename(log_json_file))[0]
    return os.path.join(output_directory, f'{filename}.offset-counts.json')


def get_offset(resource):
    re_params = re.compile(r'GET\s(?P<params>.*)\sHTTP/.*',
                           re.IGNORECASE | re.DOTALL)
    m = re_params.match(resource)
    params = m.group('params')
    params = urllib.parse.urlparse(params)
    parsed_params = urllib.parse.parse_qs(params.query)
    if 'offset' in parsed_params:
        offset = parsed_params['offset']
        assert len(offset) == 1
        return int(offset[0])


def get_offset_counts_from_log_json_file(log_json_file):
    log = read_json_file(log_json_file)
    offsets = []
    for entry in log:
        resource = entry['resource']
        try:
            offset = get_offset(resource)
            if offset:
                offsets.append(offset)
            else:
                offsets.append('not_specified')
        except Exception as e:
            print(f'Exception {e} occured with {resource}.', flush=True)
    return Counter(offsets)


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
    offset_counts = get_offset_counts_from_log_json_file(log_json_file)
    out_file_path = get_out_file_path(log_json_file, output_directory)
    print(
        f'writing {len(offset_counts)} counts to {out_file_path}', flush=True)
    write_json_file(offset_counts, out_file_path)


if __name__ == '__main__':
    main()
