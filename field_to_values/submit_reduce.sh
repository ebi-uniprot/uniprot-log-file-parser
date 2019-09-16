#!/bin/bash
MERGED_PATH=$FIELD_TO_VALUES_DIRECTORY/merged.json
MERGED_COUNTS_PATH=$FIELD_TO_VALUES_DIRECTORY/merged_counts.json

mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY

bsub \
-o $OUT_DIRECTORY/reduce.o \
-e $ERROR_DIRECTORY/reduce.e \
./reduce.py $FIELD_TO_VALUES_DIRECTORY $MERGED_PATH $MERGED_COUNTS_PATH