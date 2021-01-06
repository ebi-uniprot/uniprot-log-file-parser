#!/bin/bash
LOG_FILE_LIST=$1
RESULTS_DIRECTORY=$2
FILE=$(head -n $LSB_JOBINDEX $LOG_FILE_LIST | tail -1)
PYTHONPATH=/homes/dlrice/uniprot-log-file-parser/map.py python3 -m uniprot_log_file_parser.map $FILE $RESULTS_DIRECTORY
exit $?