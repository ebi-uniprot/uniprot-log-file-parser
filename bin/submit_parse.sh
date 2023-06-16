#!/bin/bash
source config.sh

mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY

ls /nfs/ebi/public/rw/homes/tc_uni01/uuw-stats/logs/*/*2022-03-01.log > $LOG_FILE_LIST
n=$(wc -l < $LOG_FILE_LIST)

sbatch \
--array=1-$n \
--output=$OUT_DIRECTORY/parse_%A_%a.o \
--error=$ERROR_DIRECTOR/parse_%A_%a.e \
--chdir=$MODULE_DIRECTORY \
submit_parse.batch