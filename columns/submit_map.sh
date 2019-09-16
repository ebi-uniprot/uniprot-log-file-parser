#!/bin/bash
source ../config.sh

mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY
mkdir -p $COLUMNS_DIRECTORY

find $JSON_DIRECTORY -name *.log.json > $JSON_FILE_LIST
n=$(wc -l < $JSON_FILE_LIST)

bsub -J"columns[1-$n]" \
-o $OUT_DIRECTORY/%J-%I \
-e $ERROR_DIRECTORY/%J-%I \
./map_wrapper.sh $JSON_FILE_LIST $OFFSET_DIRECTORY