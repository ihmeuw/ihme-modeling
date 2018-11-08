#!/bin/sh

source FILEPATH
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 python $*