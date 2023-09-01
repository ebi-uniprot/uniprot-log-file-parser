import os.path
import csv
from collections import defaultdict


def get_meta_path_for_log_path(meta_dir: str, log_path: str):
    return os.path.join(meta_dir, os.path.abspath(log_path)[1:].replace("/", "_"))


def in_meta(meta_dir: str, log_path: str):
    return os.path.exists(get_meta_path_for_log_path(meta_dir, log_path))


def save_meta_columns(meta_dir):
    columns = [
        "namespace",
        "log_path",
        "total_bytes",
        "lines_imported",
        "lines_skipped",
        "status_1xx",
        "status_2xx",
        "status_3xx",
        "status_4xx",
        "status_5xx",
    ]
    meta_columns_path = os.path.join(meta_dir, "columns.csv")
    if os.path.exists(meta_columns_path):
        return
    with open(meta_columns_path, "w", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)


def save_meta(
    meta_dir: str,
    namespace: str,
    log_path: str,
    total_bytes: int,
    lines_imported: int,
    lines_skipped: int,
    status_counts: defaultdict,
):
    log_meta_path = get_meta_path_for_log_path(meta_dir, log_path)
    assert not os.path.exists(log_meta_path)
    with open(log_meta_path, "w", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                namespace,
                log_path,
                total_bytes,
                lines_imported,
                lines_skipped,
            ]
            + [status_counts[f"status_{s}xx"] for s in range(1, 6)]
        )
