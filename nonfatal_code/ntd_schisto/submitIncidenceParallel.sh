#!/bin/sh
#$ -S /bin/sh
/usr/local/bin/stata-mp -q FILEPATH/incidenceParallel.do "$1" "$2" "$3" "$4" "$5" "$6"
