#!/bin/bash
source ../config.sh

mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY

MERGED_PATH=$OFFSET_DIRECTORY/merged.json

bsub \
-o $OUT_DIRECTORY/o \
-e $ERROR_DIRECTORY/e \
./reduce.py $OFFSET_DIRECTORY $MERGED_PATH