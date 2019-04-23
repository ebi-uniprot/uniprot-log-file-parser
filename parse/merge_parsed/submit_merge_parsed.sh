#!/bin/bash

mem=20000

bsub \
-e ~/error/merge_all.e \
-o ~/out/merge_all.o \
-M $mem \
-R"select[mem>$mem] rusage[mem=$mem] span[hosts=1]" \
./merge_parsed_log_json_files.sh