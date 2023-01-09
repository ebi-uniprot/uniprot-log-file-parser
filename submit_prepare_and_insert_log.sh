#!/bin/bash

out=/hps/nobackup/martin/uniprot/users/dlrice/clickhouse/log.o
error=/hps/nobackup/martin/uniprot/users/dlrice/clickhouse/log.e

mem=20000
cores=4
queue=datamover

bsub \
-q $queue \
-n $cores \
-M $mem \
-R"select[mem>$mem] rusage[mem=$mem] span[hosts=1]" \
-oo $out \
-ee $error \
./prepare_and_insert_logs_wrapper.sh