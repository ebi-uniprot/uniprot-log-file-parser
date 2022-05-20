#!/bin/bash
source config.sh

mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY
mkdir -p $MAP_DIRECTORY

find $LOG_DIRECTORY -name "*2022-03-0*.log" > $LOG_FILE_LIST
# Or use the following line for testing
# find $LOG_DIRECTORY -name *.log | head -n 10 > $LOG_FILE_LIST 
n=$(wc -l < $LOG_FILE_LIST)

mem=4000

bsub \
-J"parse[1-$n]" \
-M $mem \
-R"select[mem>$mem] rusage[mem=$mem] span[hosts=1]" \
-o $OUT_DIRECTORY/%J-%I \
-e $ERROR_DIRECTORY/%J-%I \
./map_wrapper.sh $LOG_FILE_LIST $MAP_DIRECTORY
