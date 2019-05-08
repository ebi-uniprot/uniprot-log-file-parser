#!/bin/bash
LOG_JSON_DIRECTORY=~/parsed_logs
OUT_DIRECTORY=~/jobs/out/columns
ERROR_DIRECTORY=~/jobs/error/columns
RESULTS_DIRECTORY=~/parsed_logs/columns

LOG_JSON_FILE_LIST=~/log_json_file_list.txt
find $LOG_JSON_DIRECTORY -name *.log.json > $LOG_JSON_FILE_LIST
n=$(wc -l < $LOG_JSON_FILE_LIST)
mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY
mkdir -p $RESULTS_DIRECTORY
bsub -J"column[1-$n]" \
-o $OUT_DIRECTORY/%J-%I \
-e $ERROR_DIRECTORY/%J-%I \
./map_wrapper.sh $LOG_JSON_FILE_LIST $RESULTS_DIRECTORY