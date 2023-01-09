#!/bin/bash

clickhouse=/hps/nobackup/martin/uniprot/users/dlrice/clickhouse
out=$clickhouse/log.o
error=$clickhouse/log.e

mem=20000
cores=4
queue=datamover

bsub \
-q $queue \
-n $cores \
-M $mem \
-R"select[mem>$mem] rusage[mem=$mem] span[hosts=1]" \
-o $out \
-e $error \
./prepare_and_insert_logs_wrapper.sh