#!/usr/bin/env python3
import sys
import re
from datetime import datetime
from glob import glob
import clickhouse_connect
from ua_parser import user_agent_parser


start_date = "2022-08-01"
end_date = "2022-12-31"
pattern = "/net/isilonP/public/rw/uniprot/k8s/rest/uniprotkb-rest/rest-app-uniprotkb-prod-*/logs/*.log"


def parse_log_entry(entry):
    p = re.compile(
        r"^\S+ \S+ \S+ \[(?P<datetime>[^\]]+)\] \"(?P<method>[A-Z]+) (?P<request>[^ \"]+)? HTTP/[0-9.]+\" (?P<status>[0-9]{3}) (?P<bytes>[0-9]+|-) \"(?P<referrer>[^\"]*)\" \"(?P<useragent>.*)\""
    )
    return p.match(entry)


def get_browser_family(user_agent_string):
    user_agent_parsed = user_agent_parser.ParseUserAgent(user_agent_string)
    return user_agent_parsed["family"]


def parse_bytes(bytes_string):
    try:
        if bytes_string == "-":
            return 0
        return int(float(bytes_string))
    except ValueError as error:
        print(bytes_string, error, flush=True, file=sys.stderr)
        return 0


def prepare_and_insert_log(log_path):
    columns = {
        "datetime": "DateTime",
        "method": "String",
        "request": "String",
        "status": "Int32",
        "bytes": "Int64",
        "referrer": "String",
        "useragent": "String",
        "useragentfamily": "String",
    }

    sql_columns = ",".join(
        [f"{column_name} {column_type}" for column_name, column_type in columns.items()]
    )
    column_names = columns.keys()
    column_names_set = set(column_names)

    client = clickhouse_connect.get_client(
        host="localhost", username="default", password=""
    )

    client.command(
        f"""
    CREATE TABLE IF NOT EXISTS uniprotkb
    (
    {sql_columns}
    )
    ENGINE = MergeTree()
    PRIMARY KEY (datetime, status, method, useragentfamily);
    """
    )

    client.command(
        """
    CREATE TABLE IF NOT EXISTS importedlogs
    (
    filepath String,
    n_lines_skipped Int64,
    n_total_lines Int64,
    n_lines_imported Int64
    )
    ENGINE = MergeTree()
    PRIMARY KEY (filepath);
    """
    )

    log_imported_already = client.command(
        f"SELECT COUNT(*) FROM importedlogs WHERE filepath = '{log_path}'"
    )
    if log_imported_already:
        print(log_path, f"{log_path} imported already", flush=True, file=sys.stderr)
        return

    data = []
    n_total_lines = 0
    n_lines_skipped = 0
    with open(log_path, encoding="UTF-8") as f:
        for line in f:
            n_total_lines += 1
            parsed = parse_log_entry(line.strip())
            if not parsed:
                print(log_path, "Could not parse:", line, flush=True, file=sys.stderr)
                n_lines_skipped += 1
                continue
            row = parsed.groupdict()
            try:
                parsed_datetime = datetime.strptime(
                    row["datetime"], "%d/%b/%Y:%H:%M:%S %z"
                )
            except Exception as error:
                print(log_path, error, row["datetime"], flush=True, file=sys.stderr)
                n_lines_skipped += 1
                continue
            useragentfamily = get_browser_family(row["useragent"])
            row["datetime"] = parsed_datetime
            row["useragentfamily"] = useragentfamily
            row["bytes"] = parse_bytes(row["bytes"])
            missing_column_names = column_names_set - set(row.keys())
            if missing_column_names:
                print(
                    log_path,
                    "Missing column names:",
                    missing_column_names,
                    flush=True,
                    file=sys.stderr,
                )
                n_lines_skipped += 1
                continue
            data.append([row[c] for c in column_names])
    n_lines_imported = len(data)
    print(f"{log_path} adding {n_lines_imported} rows.")
    client.insert("uniprotkb", data, column_names=column_names)

    client.insert(
        "importedlogs",
        [[log_path, n_lines_skipped, n_total_lines, n_lines_imported]],
        column_names=[
            "filepath",
            "n_lines_skipped",
            "n_total_lines",
            "n_lines_imported",
        ],
    )


def is_log_in_date_range(path):
    p = re.compile(r"\.([0-9\-]+)\.log$")
    m = p.search(path)
    assert m
    date = m.groups()[0]
    return start_date <= date <= end_date


def main():
    for path in glob(pattern):
        if is_log_in_date_range(path):
            prepare_and_insert_log(path)


if __name__ == "__main__":
    main()
