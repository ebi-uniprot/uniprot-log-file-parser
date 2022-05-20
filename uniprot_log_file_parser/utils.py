from collections import defaultdict
import os.path
import csv
import pandas as pd
import json
import msgpack


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
    # file_path = os.path.join(out_directory, out_filename)

    # df = pd.DataFrame(parsed_lines, columns=fieldnames)
    # df.to_csv(csv_file_path, index=False)
    # with open(file_path, "w", encoding="utf-8") as f:
    #     json.dump(parsed_lines, f)

    out_filename = get_out_filename(log_file_path, suffix, "msgpack")
    file_path = os.path.join(out_directory, out_filename)
    with open(file_path, "wb") as outfile:
        packed = msgpack.packb(parsed_lines)
        outfile.write(packed)

    #     writer = csv.DictWriter(f, fieldnames=fieldnames)
    #     for d in parsed_lines:
    #         writer.writerow(d)


def merge_list_defaultdicts(d1, d2):
    out = defaultdict(list)
    for d in (d1, d2):
        for k, v in d.items():
            out[k] += v[:]
    return out


def clean(text):
    return (
        text.lower()
        .strip()
        .replace('"', "")
        .replace("'", "")
        .replace("\r", "")
        .replace("\n", "")
    )


def simplify_domain(x):
    if not isinstance(x, str):
        return x
    elif "google" in x:
        return "google"
    elif "bing" in x:
        return "bing"
    elif "baidu" in x:
        return "baidu"
    elif "omim.org" in x:
        return "omim.org"
    elif "sogou.com" in x:
        return "sogou"
    elif "yahoo" in x:
        return "yahoo"
    else:
        return x
