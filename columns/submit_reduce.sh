#!/bin/bash
OUT_DIRECTORY=~/jobs/out/columns
ERROR_DIRECTORY=~/jobs/error/columns
COUNTS_DIRECTORY=~/parsed_logs/columns
MERGED_PATH=~/parsed_logs/columns/merged.json

mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY

bsub \
-o $OUT_DIRECTORY/reduce.o \
-e $ERROR_DIRECTORY/reduce.e \
./reduce.py $COUNTS_DIRECTORY $MERGED_PATH