#!/bin/sh
#$ -S /bin/sh
/usr/local/bin/stata-mp -q FILEPATH/04c_vl_nf_process_stgpr_codAge.do "$1" "$2" 
