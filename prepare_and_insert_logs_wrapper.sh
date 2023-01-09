#!/bin/bash
set -e

/hps/nobackup/martin/uniprot/users/dlrice/clickhouse/clickhouse-common-static-22.12.1.1752/usr/bin/clickhouse server --daemon &
./prepare_and_insert_logs.py
./clickhouse stop
exit $?