#!/bin/bash
source config.sh

mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY

ls /nfs/ebi/public/rw/homes/tc_uni01/uuw-stats/logs/*/*2022-03-0*.log > $LOG_FILE_LIST