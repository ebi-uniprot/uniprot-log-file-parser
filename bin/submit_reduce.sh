#!/bin/bash


bsub \
-e ~/error/reduce.e \
-o ~/out/reduce.o \
./reduce.sh