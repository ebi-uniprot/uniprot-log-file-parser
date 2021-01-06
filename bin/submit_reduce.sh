#!/bin/bash
source config.sh

mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY

bsub \
-o $OUT_DIRECTORY/reduce.o \
-e $ERROR_DIRECTORY/reduce.e \
./reduce.sh $1
