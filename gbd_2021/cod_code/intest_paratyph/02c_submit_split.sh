#!/bin/sh
#$ -S /bin/sh
/FILEPATH/stata-mp -q /FILEPATH/02d_split.do "$1" "$2" "$3" "$4" "$5"
