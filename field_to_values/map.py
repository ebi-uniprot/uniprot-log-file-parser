#!/usr/bin/env python3
from glob import glob
import os.path
import json
import argparse
from collections import defaultdict, Counter
import urllib.parse
import luqum.parser
import luqum.tree
import re


def read_json_file(json_file):
    with open(json_file) as f:
        return json.load(f)


def write_json_file(obj, file_path):
    with open(file_path, 'w') as f:
        json.dump(obj, f, indent=4)


def get_out_file_path(log_json_file, output_directory):
    filename = os.path.splitext(os.path.basename(log_json_file))[0]
    return os.path.join(output_directory, f'{filename}.field-to-values.json')


def merge_list_defaultdicts(d1, d2):
    out = defaultdict(list)
    for d in (d1, d2):
        for k, v in d.items():
            out[k] += v[:]
    return out


def get_query(resource):
    re_params = re.compile(r'GET\s(?P<params>.*)\sHTTP/.*',
                           re.IGNORECASE | re.DOTALL)
    m = re_params.match(resource)
    assert m
    params = m.group('params')
    params = urllib.parse.urlparse(params)
    parsed_params = urllib.parse.parse_qs(params.query)
    if 'query' in parsed_params:
        query = parsed_params['query']
        assert len(query) == 1
        return query[0]


def get_lucene_query_tree(query):
    unquoted_query = urllib.parse.unquote_plus(
        urllib.parse.unquote_plus(query))
    return luqum.parser.parser.parse(unquoted_query)


def get_field_to_value_counts_from_query(query):
    tree = get_lucene_query_tree(query)
    return get_field_to_value_counts_from_query_tree(tree)


def clean(text):
    return text.lower().strip().replace('"', '').replace('\'', '')


def get_field_to_value_counts_from_query_tree(tree):
    field_to_values = defaultdict(list)
    if hasattr(tree, 'name'):
        field = clean(tree.name)
        value = clean(tree.expr.value)
        field_to_values[field].append(value)
    if tree.children:
        for child in tree.children:
            _field_to_values = get_field_to_value_counts_from_query_tree(child)
            field_to_values = merge_list_defaultdicts(
                _field_to_values, field_to_values)
    return field_to_values


def get_field_to_value_counts_from_log_json_file(log_json_file):
    log = read_json_file(log_json_file)
    field_to_values = defaultdict(list)
    for entry in log:
        resource = entry['resource']
        try:
            query = get_query(resource)
            _field_to_values = get_field_to_value_counts_from_query(query)
            field_to_values = merge_list_defaultdicts(
                field_to_values, _field_to_values)
        except Exception as e:
            print(f'Exception {e} occured with {resource}.')
    return {field: Counter(values) for field, values in field_to_values.items()}


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
    field_to_value_counts_json_file_path = get_field_to_value_counts_from_log_json_file(
        log_json_file)
    out_file_path = get_out_file_path(log_json_file, output_directory)
    print(
        f'writing {len(field_to_value_counts_json_file_path)} field-value counts to {out_file_path}', flush=True)
    write_json_file(field_to_value_counts_json_file_path, out_file_path)


if __name__ == '__main__':
    main()
