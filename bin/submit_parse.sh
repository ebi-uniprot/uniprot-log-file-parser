#!/bin/bash
source config.sh

mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY

ls /nfs/ebi/public/rw/homes/tc_uni01/uuw-stats/logs/*/*2022-03-01.log > $LOG_FILE_LIST
n=$(wc -l < $LOG_FILE_LIST)

sbatch \
--time 2:00:00 \
--cpus-per-task 1 \
--partition datamover \
--array=1-$n \
--chdir=$MODULE_DIRECTORY \
--ntasks=1 \
--mem=6G \
parse_array_task.batch