#!/bin/sh
#$ -S /bin/sh
export STATA_DO="do \"$1\""
FILEPATH -q $@
echo $1 $2 
