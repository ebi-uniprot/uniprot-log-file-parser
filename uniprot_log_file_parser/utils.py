import os.path
import csv
import json


def get_root_out_filename(log_file_path):
    _, filename = os.path.split(log_file_path)
    return filename


def get_out_filename(log_file_path, suffix, ext):
    root_out_filename = get_root_out_filename(log_file_path)
    return f"{root_out_filename}.{suffix}.{ext}"


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

