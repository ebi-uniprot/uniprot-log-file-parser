#!/usr/bin/env python3
import argparse
from uniprot_log_file_parser.db import backup_database, get_db_connection


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "dest_dir",
        type=str,
        help="The destination directory for the backup"
    )
    parser.add_argument(
        "db_path",
        type=str,
        help="The path to the Duckdbc file to hold the parsed log data",
    )
    args = parser.parse_args()
    return args.dest_dir, args.db_path


def main():
    dest_dir, db_path = get_arguments()
    dbc = get_db_connection(db_path)
    backup_database(dbc, dest_dir)


if __name__ == "__main__":
    main()
