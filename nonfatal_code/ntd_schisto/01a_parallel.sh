#!/bin/sh
#$ -S /bin/sh
/usr/local/bin/stata-mp -q FILEPATH.do "$1"
