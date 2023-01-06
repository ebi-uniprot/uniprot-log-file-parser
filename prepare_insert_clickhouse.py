#!/usr/bin/env python3
import sys
import re
from datetime import datetime
import clickhouse_connect
from ua_parser import user_agent_parser


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


def main():
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

    log_file_path = sys.argv[1]
    data = []
    with open(log_file_path, encoding="UTF-8") as f:
        for line in f:
            parsed = parse_log_entry(line.strip())
            if not parsed:
                print("Could not parse:", line, flush=True, file=sys.stderr)
                continue
            row = parsed.groupdict()
            try:
                parsed_datetime = datetime.strptime(
                    row["datetime"], "%d/%b/%Y:%H:%M:%S %z"
                )
            except Exception as error:
                print(error, row["datetime"], flush=True, file=sys.stderr)
            useragentfamily = get_browser_family(row["useragent"])
            row["datetime"] = parsed_datetime
            row["useragentfamily"] = useragentfamily
            row["bytes"] = parse_bytes(row["bytes"])
            missing_column_names = column_names_set - set(row.keys())
            if missing_column_names:
                print(
                    "Missing column names:",
                    missing_column_names,
                    flush=True,
                    file=sys.stderr,
                )
            data.append([row[c] for c in column_names])

    print(f"Adding {len(data)} rows.")
    client.insert("uniprotkb", data, column_names=column_names)


if __name__ == "__main__":
    main()
