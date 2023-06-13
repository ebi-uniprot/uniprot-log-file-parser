import os.path
import csv
from collections import defaultdict
import duckdb


def in_meta(
    meta_path: str, log_path: str
):
    if not os.path.isfile(meta_path):
        return False
    return bool(
        duckdb.sql(
            f"SELECT log_path FROM read_csv_auto('{meta_path}', header=True) WHERE log_path = '{log_path}'"
        )
    )


def save_meta(
    meta_path: str,
    namespace: str,
    log_path: str,
    total_bytes: int,
    lines_imported: int,
    lines_skipped: int,
    status_counts: defaultdict,
):
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
    if not os.path.isfile(meta_path):
        with open(meta_path, "w", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
    with open(meta_path, "a", encoding="utf-8") as f:
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
