#!/bin/bash
FILE_LIST=$1
FILE=$(head -n $LSB_JOBINDEX $FILE_LIST | tail -1)
PYTHONPATH=/homes/dlrice/uniprot-log-file-parser python3 -m uniprot_log_file_parser.find_version_ext $FILE
exit $?