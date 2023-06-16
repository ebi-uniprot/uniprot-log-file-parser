#!/bin/bash
source config.sh

n=$(wc -l < $LOG_FILE_LIST)

sbatch \
--array=1-$n \
--output=$OUT_DIRECTORY/parse_%A_%a.o \
--error=$ERROR_DIRECTOR/parse_%A_%a.e \
--chdir=$MODULE_DIRECTORY \
submit_parse.batch