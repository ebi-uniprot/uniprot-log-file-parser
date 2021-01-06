#!/bin/bash
source ../config.sh

mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY
mkdir -p $MAP_DIRECTORY

find $LOG_DIRECTORY -name *.log > $LOG_FILE_LIST
n=$(wc -l < $LOG_FILE_LIST)

bsub \
-J"parse[1-$n]" \
-o $OUT_DIRECTORY/%J-%I \
-e $ERROR_DIRECTORY/%J-%I \
./map_wrapper.sh $LOG_FILE_LIST $MAP_DIRECTORY