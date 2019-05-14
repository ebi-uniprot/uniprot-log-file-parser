#!/bin/bash
LOG_DIRECTORY=/nfs/public/rw/homes/tc_uni01/uuw-stats/logs/
RESULTS_DIRECTORY=~/log_parsing/browser_entries

LOG_FILE_LIST=~/log_file_list.txt
JOBS_DIRECTORY=~/jobs/

DATETIME=$(date '+%Y.%m.%d-%H.%M.%S')
OUT_DIRECTORY=$JOBS_DIRECTORY/$DATETIME/out
ERROR_DIRECTORY=$JOBS_DIRECTORY/$DATETIME/error

mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY
mkdir -p $RESULTS_DIRECTORY

find $LOG_DIRECTORY -name *.log > $LOG_FILE_LIST
n=$(wc -l < $LOG_FILE_LIST)
bsub \
-J"parse[1-$n]" \
-o $OUT_DIRECTORY/%J-%I \
-e $ERROR_DIRECTORY/%J-%I \
./map_wrapper.sh $LOG_FILE_LIST $RESULTS_DIRECTORY