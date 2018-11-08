#!/bin/sh
#$ -S /bin/sh
FILEPATH/stata-mp -q FILEPATH/deaths2cases.do "$1" "$2" "$3"  
