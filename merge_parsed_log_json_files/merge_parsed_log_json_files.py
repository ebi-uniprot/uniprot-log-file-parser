#!/usr/bin/env python3
from glob import glob
import os.path
import json
import argparse


def read_json_file(json_file):
    with open(json_file) as f:
        return json.load(f)


def write_to_json_file(obj, file_path):
    with open(file_path, 'w') as f:
        json.dump(obj, f, indent=4)


def merge_parsed_log_json_files(parsed_log_directory, merged_parsed_log_path):
    pattern = '*.log.json'
    parsed_logs = glob(os.path.join(parsed_log_directory, pattern))
    all_parsed = []
    for i, parsed_log in enumerate(parsed_logs):
        all_parsed += read_json_file(parsed_log)
        print(i, len(all_parsed))
    write_to_json_file(all_parsed, merged_parsed_log_path)


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('parsed_log_directory', type=str,
                        help='The directory containing json files of log file entries with filenames of the pattern *.log.json')
    parser.add_argument('merged_parsed_log_path', type=str,
                        help='The path where the resulting json file of merged parsed logs will be saved')
    args = parser.parse_args()
    return args.parsed_log_directory, args.merged_parsed_log_path


def main():
    parsed_log_directory, merged_parsed_log_path = get_arguments()
    merge_parsed_log_json_files(parsed_log_directory, merged_parsed_log_path)


if __name__ == '__main__':
    main()
