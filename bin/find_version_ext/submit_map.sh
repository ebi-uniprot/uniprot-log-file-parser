#!/bin/bash

CWD=/nfs/public/rw/homes/uni_adm/tmp/log_parsing/parsed-no-bots
EXT_DIRECTORY=$CWD/version-ext
OUT_DIRECTORY=$EXT_DIRECTORY/out
ERROR_DIRECTORY=$EXT_DIRECTORY/error

mkdir -p $EXT_DIRECTORY
mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY

PARSED_DIRECTORY=$CWD/parsed-by-date
FILE_LIST=$EXT_DIRECTORY/file_list.txt

find $PARSED_DIRECTORY -name 2020*.csv > $FILE_LIST
n=$(wc -l < $FILE_LIST)

bsub \
-J"ext[1-$n]" \
-o $OUT_DIRECTORY/%J-%I \
-e $ERROR_DIRECTORY/%J-%I \
./map_wrapper.sh $FILE_LIST 