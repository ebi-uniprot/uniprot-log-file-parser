#!/bin/bash
LOG_PARSING_DIRECTORY=/net/isilonP/public/rw/homes/uni_adm/tmp/log_parsing/
JSON_DIRECTORY=$LOG_PARSING_DIRECTORY/json
COLUMNS_DIRECTORY=$LOG_PARSING_DIRECTORY/columns
OFFSET_DIRECTORY=$LOG_PARSING_DIRECTORY/offset
JOBS_DIRECTORY=~/jobs/
DATETIME=$(date '+%Y.%m.%d-%H.%M.%S')
OUT_DIRECTORY=$JOBS_DIRECTORY/$DATETIME/out
ERROR_DIRECTORY=$JOBS_DIRECTORY/$DATETIME/error
LOG_DIRECTORY=/nfs/public/rw/homes/tc_uni01/uuw-stats/logs/
LOG_FILE_LIST=~/log_file_list.txt
JSON_FILE_LIST=~/json_file_list.txt