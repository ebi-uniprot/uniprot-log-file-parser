#!/bin/bash
source ../config.sh

mkdir -p $REDUCE_DIRECTORY

cat $MAP_DIRECTORY/*.bytes.csv > $REDUCE_DIRECTORY/bytes.csv
cat $MAP_DIRECTORY/*.n-requests.csv > $REDUCE_DIRECTORY/n-requests.csv
cat $MAP_DIRECTORY/*.parsed.csv > $REDUCE_DIRECTORY/parsed.csv
cat $MAP_DIRECTORY/*.user-type.csv > $REDUCE_DIRECTORY/user-type.csv
