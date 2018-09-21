#!/bin/sh
#$ -S /bin/sh
/usr/local/bin/stata-mp -q FILEPATH/c_processModel.do "$1"
