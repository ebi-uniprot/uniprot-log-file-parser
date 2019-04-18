#!/bin/bash
LOGS=/homes/dlrice/uniprot-log-file-parser/uuw-stats-logs.txt
file=$(head -n $LSB_JOBINDEX $LOGS | tail -1)
PARSER_PATH=/homes/dlrice/uniprot-log-file-parser/uniprot_log_file_parser.py
OUT_DIR=/homes/dlrice/parsed_logs
python $PARSER_PATH $file $OUT_DIR
exit $?