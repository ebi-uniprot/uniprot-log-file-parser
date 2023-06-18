#!/bin/bash
source config.sh

mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY

ls /nfs/ebi/public/rw/homes/tc_uni01/uuw-stats/logs/*/*2022-03-*.log > $LOG_FILE_LIST
n=$(wc -l < $LOG_FILE_LIST)

sbatch \
--time 1:00:00 \
--cpus-per-task 1 \
--partition datamover \
--array=1-$n \
--chdir=$MODULE_DIRECTORY \
--output=$OUT_DIRECTORY/parse_%A_%a.o \
--error=$ERROR_DIRECTORY/parse_%A_%a.e \
--ntasks=1 \
--mem=6G \
parse_array_task.batch