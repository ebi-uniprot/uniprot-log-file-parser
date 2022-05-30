#!/bin/bash
DATETIME=$(date '+%Y.%m.%d-%H.%M.%S')
LOG_PARSING_DIRECTORY=/net/isilonP/public/rw/homes/uni_adm/tmp/log_parsing/parquet
MAP_DIRECTORY=$LOG_PARSING_DIRECTORY/map
OUT_DIRECTORY=$LOG_PARSING_DIRECTORY/out
ERROR_DIRECTORY=$LOG_PARSING_DIRECTORY/error
REDUCE_DIRECTORY=$LOG_PARSING_DIRECTORY/reduce
LOG_FILE_LIST=$LOG_PARSING_DIRECTORY/log_file_list.txt
LOG_DIRECTORY=/nfs/public/rw/homes/tc_uni01/uuw-stats/logs/