#!/bin/sh
#$ -S /bin/sh
umask 002
export LD_LIBRARY_PATH=/usr/lib64
export STATA_DO="do \"$1\""
/usr/local/stata13/stata-mp -q $STATA_DO $2
