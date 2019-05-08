#!/usr/bin/env python3
from glob import glob
import os.path
import json
import argparse
from collections import Counter


def read_json_file(json_file):
    with open(json_file) as f:
        return json.load(f)


def write_json_file(obj, file_path):
    with open(file_path, 'w') as f:
        json.dump(obj, f, indent=4)


def merge_files(offset_counts_directory, merged_path):
    pattern = '*.number-column-counts.json'
    files = glob(os.path.join(offset_counts_directory, pattern))
    merged = Counter()
    for f in files:
        counts = Counter(read_json_file(f))
        merged += counts
    write_json_file(merged, merged_path)


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('number_column_counts_directory', type=str,
                        help='The directory containing the json offset count files filenames of the pattern *.number-column-counts.json')
    parser.add_argument('merged_path', type=str,
                        help='The path where the resulting json file will be saved')
    args = parser.parse_args()
    return args.number_column_counts_directory, args.merged_path


def main():
    offset_counts_directory, merged_path = get_arguments()
    merge_files(offset_counts_directory, merged_path)


if __name__ == '__main__':
    main()
