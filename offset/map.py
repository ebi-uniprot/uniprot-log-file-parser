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
    return os.path.join(output_directory, f'{filename}.offset-counts.json')


def get_params(url):
    params = urllib.parse.urlparse(url)
    return urllib.parse.parse_qs(params.query)


def get_url_from_resource(resource):
    p = re.compile(r'GET\s(?P<url>.*)\sHTTP/.*',
                   re.IGNORECASE | re.DOTALL)
    m = p.match(resource)
    return m.group('url')


def get_offset(params):
    if 'offset' in params:
        offset = params['offset']
        assert len(offset) == 1
        return int(offset[0])
    else:
        return 0


def equal_sans_offset(params1, params2):
    keys1 = params1.keys()
    keys2 = params2.keys()
    try:
        keys1.remove('offset')
        keys2.remove('offset')
    except:
        pass
    if keys1 != keys2:
        return False
    for key in keys1:
        value1 = params1[key]
        value2 = params1[key]
        if value1 != value2:
            return False
    return True


def safe_remove(s, k):
    try:
        return s.remove(k)
    except:
        return s


def is_pagination_request(params1, params2):
    keys1 = set(params1.keys())
    keys2 = set(params2.keys())
    safe_remove(keys1, 'columns')
    safe_remove(keys1, 'offset')
    safe_remove(keys2, 'columns')
    safe_remove(keys2, 'offset')
    if keys1 != keys2:
        return False
    for key in keys1 | keys2:
        value1 = params1[key]
        value2 = params1[key]
        if value1 != value2:
            return False
    offset1 = get_offset(params1)
    offset2 = get_offset(params2)
    return offset1 != offset2


def get_offset_counts_from_log_json_file(log_json_file):
    log = read_json_file(log_json_file)
    offsets = []
    for entry in log:
        resource = entry['resource']
        referer = entry['referer']
        try:
            url = get_url_from_resource(resource)
            resource_params = get_params(url)
            resource_offset = get_offset(resource_params)
            if resource_offset:
                referer_params = get_params(referer)
                if is_pagination_request(resource_params, referer_params):
                    offsets.append(resource_offset)
            else:
                offsets.append('not_specified')
        except Exception as e:
            print(f'Exception {e} occured with {resource}.',
                  flush=True, file=sys.stderr)
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
