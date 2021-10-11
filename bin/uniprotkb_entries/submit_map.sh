#!/bin/bash

CWD=/nfs/public/rw/homes/uni_adm/tmp/log_parsing/parsed-no-bots
WORK_DIRECTORY=$CWD/uniprotk-entries
OUT_DIRECTORY=$WORK_DIRECTORY/out
ERROR_DIRECTORY=$WORK_DIRECTORY/error

mkdir -p $WORK_DIRECTORY
mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY

PARSED_DIRECTORY=$CWD/parsed-by-date
FILE_LIST=$WORK_DIRECTORY/file_list.txt

find $PARSED_DIRECTORY -name 2020*.csv > $FILE_LIST
n=$(wc -l < $FILE_LIST)

mem=4000

bsub \
-J"uniprotkb[1-$n]" \
-M $mem \
-R"select[mem>$mem] rusage[mem=$mem] span[hosts=1]" \
-o $OUT_DIRECTORY/%J-%I \
-e $ERROR_DIRECTORY/%J-%I \
./map_wrapper.sh $FILE_LIST 
