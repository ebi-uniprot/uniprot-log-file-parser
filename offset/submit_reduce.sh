#!/bin/bash
OUT_DIRECTORY=~/jobs/out/offset
ERROR_DIRECTORY=~/jobs/error/offset
OFFSET_COUNTS_DIRECTORY=~/parsed_logs/offset
MERGED_PATH=~/parsed_logs/offset/merged.json

mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY

bsub \
-o $OUT_DIRECTORY/reduce.o \
-e $ERROR_DIRECTORY/reduce.e \
./reduce.py $OFFSET_COUNTS_DIRECTORY $MERGED_PATH