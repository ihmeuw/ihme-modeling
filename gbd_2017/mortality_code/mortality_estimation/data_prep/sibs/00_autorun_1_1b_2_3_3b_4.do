**********************************************************************************************************************************************************************                          
**  Adult Mortality through Sibling Histories: #0. AUTO RUN ENTIRE ANALYSIS
**
**  Authors:
**
**  Decription: This do-file automatically runs all analytic do files, in order
**
**
*********************************************************************************************************************************************************************
*********************************************************************************************************************************************************************

clear all
set mem 15g
set more off
set matsize 5000
capture log close
pause off

local user "`c(username)'"

** Set directories - globals to apply to all do files
if c(os)=="Windows" {
     local prefix="FILEPATH"
     local homeprefix="FILEPATH"
    global codedir "FILEPATH"
    }
if c(os)=="Unix" {
    local prefix="FILEPATH"
     local homeprefix="FILEPATH"
    global codedir "FILEPATH"
    }

local iso3 = "`1'"
local year = "`2'"
local filename = "`3'"  
local nid = "`4'"
local subnat = "`5'"
local mapping = "`6'"
local province = "`7'"

global datadir "FILEPATH"

local date = c(current_date)

** Run do-files
// set current directory to call do-files from
cd "FILEPATH"
do "01_extracting_sib_info_from_all_surveys - with HHID.do"
extract_sib, iso3("`iso3'") year("`year'") filename("`filename'") subnat("`subnat'") mapping("`mapping'") province("`province'")
cd "FILEPATH"
do "02_zero_survivor_correction.do"
cd "FILEPATH"
do "03_data_prep_for_analysis.do"
cd "FILEPATH"
do "03b_data_prep_for_analysis.do"
cd "FILEPATH"
do "04_regression_svy.do"
run_regression, iso3("`iso3'") year("`year'")
cd "FILEPATH"
do "05_compile_results_svy.do"
do "05_compile_results_svy_subnat.do"
if "`subnat'" == "nan" {
    compile_results, iso3("`iso3'") year("`year'") nid("`nid'")
}
else {
    compile_results_subnat, iso3("`iso3'") year("`year'") nid("`nid'")
}

exit
