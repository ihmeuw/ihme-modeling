#!/bin/sh
#$ -S /bin/sh
echo $@

# Launch Stata
FILEPATH -q do \"$1\" $2 $3 $4
