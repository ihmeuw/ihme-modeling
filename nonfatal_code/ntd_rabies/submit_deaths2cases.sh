#!/bin/sh
#$ -S /bin/sh
/usr/local/bin/stata-mp -q FILEPATH/deaths2cases.do "$1" "$2" "$3"  
