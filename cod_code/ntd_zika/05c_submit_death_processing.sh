#!/bin/sh
#$ -S /bin/sh
/usr/local/bin/stata-mp -q FILEPATH/05d_death_processing.do "$1"  
