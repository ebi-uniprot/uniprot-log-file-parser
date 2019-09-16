#!/bin/bash
MERGED_PATH=$COLUMNS_DIRECTORY/merged.json

mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY

bsub \
-o $OUT_DIRECTORY/reduce.o \
-e $ERROR_DIRECTORY/reduce.e \
./reduce.py $COLUMNS_DIRECTORY $MERGED_PATH