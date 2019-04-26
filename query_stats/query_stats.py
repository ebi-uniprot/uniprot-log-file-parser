#!/usr/bin/env python3
import json
import urllib.parse
import luqum.parser
import luqum.tree
from collections import defaultdict, Counter
from luqum.utils import UnknownOperationResolver
import re
import os.path
import argparse


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


def get_query_stats_from_query(query):
    tree = get_lucene_query_tree(query)
    return get_query_stats_from_query_tree(tree)


def get_query_stats_from_query_tree(tree, n_terms=0, n_and=0, n_or=0, n_not=0):
    if type(tree) in [luqum.tree.Phrase, luqum.tree.Word]:
        n_terms += 1
    elif type(tree) == luqum.tree.AndOperation:
        n_and += 1
    elif type(tree) == luqum.tree.OrOperation:
        n_or += 1
    elif type(tree) == luqum.tree.Not:
        n_not += 1
    if tree.children:
        for child in tree.children:
            _n_terms, _n_and, _n_or, _n_not = get_query_stats_from_query_tree(
                child)
            n_terms += _n_terms
            n_and += _n_and
            n_or += _n_or
            n_not += _n_not
    return n_terms, n_and, n_or, n_not


def stringify_counts(counts):
    return '_'.join((str(el) for el in counts))


def write_to_json_file(obj, file_path):
    with open(file_path, 'w') as f:
        json.dump(obj, f, indent=4)


def read_json_file(json_file):
    with open(json_file) as f:
        return json.load(f)


def get_query_stats_counts_from_log_json_file(log_json_file):
    log = read_json_file(log_json_file)
    counts = defaultdict(int)
    for entry in log:
        resource = entry['resource']
        try:
            query = get_query(resource)
            _counts = get_query_stats_from_query(query)
            counts[stringify_counts(_counts)] += 1
        except Exception as e:
            print(f'Exception {e} occured with {resource}.', flush=True)
    return counts


def get_out_file_path(log_json_file, output_directory):
    filename = os.path.splitext(os.path.basename(log_json_file))[0]
    return os.path.join(output_directory, f'{filename}.query-stats-counts.json')


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
    counts = get_query_stats_counts_from_log_json_file(log_json_file)
    out_file_path = get_out_file_path(log_json_file, output_directory)
    print(
        f'writing {len(counts)} counts to {out_file_path}', flush=True)
    write_to_json_file(counts, out_file_path)


if __name__ == '__main__':
    main()
