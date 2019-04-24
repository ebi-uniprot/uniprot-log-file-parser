#!/bin/bash
LOG_JSON_DIRECTORY=~/parsed_logs
LOG_JSON_FILE_LIST=~/log_json_file_list.txt
find $LOG_JSON_DIRECTORY -name *.log.json > $LOG_JSON_FILE_LIST
n=$(wc -l < $LOG_JSON_FILE_LIST)
OUT_DIRECTORY=~/out/offset
ERROR_DIRECTORY=~/error/offset
RESULTS_DIRECTORY=~/parsed_logs/offset
mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY
mkdir -p $RESULTS_DIRECTORY
bsub -J"offset[1-$n]" \
-o $OUT_DIRECTORY/o.%J-%I \
-e $ERROR_DIRECTORY/e.%J-%I \
./wrapper.sh $LOG_JSON_FILE_LIST $RESULTS_DIRECTORY