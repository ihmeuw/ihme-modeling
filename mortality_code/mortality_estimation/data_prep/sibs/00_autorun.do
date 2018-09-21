** *******************************************************************************************************************************************************************							
**  Adult Mortality through Sibling Histories: #0. AUTO RUN ENTIRE ANALYSIS
**
**	Decription: This do-file automatically runs all analytic do files, in order
**
** 	NOTE: IHME OWNS THE COPYRIGHT 
**
** *******************************************************************************************************************************************************************

clear
capture clear matrix
set more off
set mem 500m
pause on	
capture log close

global datadir "strDataDirectory"
global resultsdir "strResultsDirectory"
global logdir "strLogDirectory"
global indir "strRawDataDirectory"
global locdir "strLocationCodeDirectory"

log using "$datadir/00_autorun_`date'", replace

cd "$codedir"
do "01_format_SURVEY.do"
do "02_zero_survivor_correction_FEMALE_RESPONDENTS.do"
do "03_data_prep_for_analysis.do"
do "03b_data_prep_for_analysis.do"
do "04_regression_svy.do"
do "05_compile_results_svy.do"

log close
