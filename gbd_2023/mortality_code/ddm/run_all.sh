#!/bin/sh
#$ -S /bin/sh
export STATA_DO="do \"$1\""
FILEPATH/stata-mp -q $@
echo $1 $2 

