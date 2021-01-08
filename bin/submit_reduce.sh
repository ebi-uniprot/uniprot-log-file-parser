#!/bin/bash
if [ $# -eq 0 ]; then
  echo "Output directory not supplied. Exiting."
  exit 1
fi

OUT_DIRECTORY=$1/out
ERROR_DIRECTORY=$1/error

if [ ! -d "$OUT_DIRECTORY" ]; then
  exit 1 
fi

if [ ! -d "$ERROR_DIRECTORY" ]; then
  exit 1 
fi

bsub \
-o $OUT_DIRECTORY/reduce.o \
-e $ERROR_DIRECTORY/reduce.e \
./reduce.sh $1
