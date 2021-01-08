from collections import defaultdict
import os.path
import csv


def get_root_out_filename(log_file_path):
    file_path, filename = os.path.split(log_file_path)
    _, directory = os.path.split(file_path)
    return f'{directory}__{filename}'


def get_csv_out_filename(log_file_path, suffix):
    root_out_filename = get_root_out_filename(log_file_path)
    return f'{root_out_filename}.{suffix}.csv'


def write_counts_to_csv(out_directory, log_file_path, suffix, count_dict):
    csv_out_filename = get_csv_out_filename(log_file_path, suffix)
    csv_file_path = os.path.join(out_directory, csv_out_filename)
    with open(csv_file_path, 'w') as f:
        writer = csv.writer(f)
        for k, v in count_dict.items():
            writer.writerow([k, v])


def write_parsed_lines_to_csv(out_directory, log_file_path, suffix, parsed_lines, fieldnames):
    csv_out_filename = get_csv_out_filename(log_file_path, suffix)
    csv_file_path = os.path.join(out_directory, csv_out_filename)
    with open(csv_file_path, 'w', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        for d in parsed_lines:
            writer.writerow(d)


def merge_list_defaultdicts(d1, d2):
    out = defaultdict(list)
    for d in (d1, d2):
        for k, v in d.items():
            out[k] += v[:]
    return out


def clean(text):
    return text.lower().strip().replace('"', '').replace('\'', '').replace('\r', '').replace('\n', '')
