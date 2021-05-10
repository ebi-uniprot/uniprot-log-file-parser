#!/bin/bash
if [ $# -eq 0 ]; then
  echo "Output directory not supplied. Exiting."
  exit 1
fi

MAP_DIRECTORY=$1/map
REDUCE_DIRECTORY=$1/reduce

if [ ! -d "$MAP_DIRECTORY" ]; then
  exit 1 
fi

mkdir -p $REDUCE_DIRECTORY

cat $MAP_DIRECTORY/*.bytes.csv > $REDUCE_DIRECTORY/bytes.csv
cat $MAP_DIRECTORY/*.n-requests.csv > $REDUCE_DIRECTORY/n-requests.csv
cat $MAP_DIRECTORY/*.parsed.csv > $REDUCE_DIRECTORY/parsed.csv
cat $MAP_DIRECTORY/*.field-names.csv > $REDUCE_DIRECTORY/field-names.csv
cat $MAP_DIRECTORY/*.n-fields.csv > $REDUCE_DIRECTORY/n-fields.csv
