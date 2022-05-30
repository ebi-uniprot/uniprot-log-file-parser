import os.path
import csv
import json
import re


def get_out_filename(log_file_path, suffix, ext):
    filepath, filename = os.path.split(log_file_path)
    _, server = os.path.split(filepath)
    return f"{server}-{filename}.{suffix}.{ext}"


def get_date_from_filename(log_file_path):
    p = re.compile(r"access_(?P<date>[0-9\-]+)\.log")
    _, filename = os.path.split(log_file_path)
    m = p.match(filename)
    if m:
        return m.group("date")


def write_counts_to_csv(out_directory, log_file_path, suffix, count_dict):
    csv_out_filename = get_out_filename(log_file_path, suffix, "csv")
    csv_file_path = os.path.join(out_directory, csv_out_filename)
    with open(csv_file_path, "w") as f:
        writer = csv.writer(f)
        for k, v in count_dict.items():
            writer.writerow([k, v])


def write_parsed_lines(out_directory, log_file_path, suffix, parsed_lines):
    out_filename = get_out_filename(log_file_path, suffix, "json")
    file_path = os.path.join(out_directory, out_filename)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(parsed_lines, f)

