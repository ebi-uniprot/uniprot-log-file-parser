#!/bin/bash
OUT_PATH=~/jobs/out/offset
ERROR_DIRECTORY=~/jobs/error/offset
OFFSET_COUNTS_DIRECTORY=~/parsed_logs/offset
MERGED_PATH=~/jobs/out/offset/merged.json

mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY
mkdir -p $RESULTS_DIRECTORY

bsub \
-o $OUT_DIRECTORY/%J-%I \
-e $ERROR_DIRECTORY/%J-%I \
./reduce.py $OFFSET_COUNTS_DIRECTORY $MERGED_PATH