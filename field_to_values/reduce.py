#!/usr/bin/env python3
from glob import glob
import os.path
import json
import argparse
from collections import Counter, defaultdict


def read_json_file(json_file):
    with open(json_file) as f:
        return json.load(f)


def write_json_file(obj, file_path):
    with open(file_path, 'w') as f:
        json.dump(obj, f, indent=4)


def merge_files(field_to_values_directory, merged_path, merged_counts_path):
    pattern = '*.field-to-values.json'
    files = glob(os.path.join(field_to_values_directory, pattern))
    all_counts = Counter()
    for f in files:
        all_counts += Counter(read_json_file(f))
    write_json_file(all_counts, merged_path)
    field_count = {field: sum(counts.values())
                   for field, counts in all_counts.items()}
    write_json_file(field_count, merged_counts_path)


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('field_to_values_directory', type=str,
                        help='The directory containing the json offset count files filenames of the pattern *.number-column-counts.json')
    parser.add_argument('merged_path', type=str,
                        help='The path where the resulting json file will be saved')
    parser.add_argument('merged_counts_path', type=str,
                        help='The path where the resulting json file of field counts will be saved')
    args = parser.parse_args()
    return args.field_to_values_directory, args.merged_path, args.merged_counts_path


def main():
    field_to_values_directory, merged_path, merged_counts_path = get_arguments()
    merge_files(field_to_values_directory, merged_path, merged_counts_path)


if __name__ == '__main__':
    main()
