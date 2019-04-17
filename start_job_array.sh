#!/bin/bash
LOGS=/homes/dlrice/parsed_logs/uuw-stats-logs.txt
n=$(wc -l < $LOGS)
bsub -J"logparse[1-$n]" -o ~/out/logparse.o.%J-%I -e ~/error/logparse.e.%J-%I ./wrapper.sh