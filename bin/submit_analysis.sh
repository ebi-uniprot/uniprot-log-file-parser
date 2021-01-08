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

mem=64000

bsub \
-o $OUT_DIRECTORY/analysis.o \
-e $ERROR_DIRECTORY/analysis.e \
-M $mem \
-R"select[mem>$mem] rusage[mem=$mem] span[hosts=1]" \
jupyter nbconvert --to notebook --inplace --ExecutePreprocessor.timeout=-1 --execute ../notebooks/analysis.ipynb
