#!/bin/sh
#$ -S /bin/sh
/usr/local/bin/stata-mp -q /homes/hcg1/repos/ntd_models/ntd_models/ntd_rabies/step_1b_deaths2cases.do "$1" "$2" "$3" "$4" 
