#!/bin/bash

CWD=/nfs/public/rw/homes/uni_adm/tmp/log_parsing/parsed-no-bots
WORK_DIRECTORY=$CWD/uniprotkb-entries
OUT_DIRECTORY=$WORK_DIRECTORY/out
ERROR_DIRECTORY=$WORK_DIRECTORY/error

mkdir -p $WORK_DIRECTORY
mkdir -p $OUT_DIRECTORY
mkdir -p $ERROR_DIRECTORY


mem=10000

bsub \
-J"uniprotkb" \
-M $mem \
-R"select[mem>$mem] rusage[mem=$mem] span[hosts=1]" \
-o $OUT_DIRECTORY/reduce.o \
-e $ERROR_DIRECTORY/reduce.e \
PYTHONPATH=/homes/dlrice/uniprot-log-file-parser python3 -m uniprot_log_file_parser.uniprotkb_entries_reduce 
