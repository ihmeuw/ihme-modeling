
if c(os) == "Unix" {
	local prefix "FILEPATH"
	set more off
	set odbcmgr unixodbc
}
else if c(os) == "Windows" {
	local prefix "FILEPATH"
}

clear all
set more off
cap restore, not
local root "FILEPATH"
local fbd_version "`1'"
import delimited using "FILEPATH/run_versions.csv",clear varn(1)
keep if fbd_version == "`fbd_version'"
local gbd_version = gbd_version
local draws = draws
local spec_dir "FILEPATH"
di "`fbd_version'"

local cluster: env SGE_CLUSTER_NAME
di "`cluster'"
local flag ""
if "`cluster'" == "prod" {
local flag "-P proj_forecasting"
}

*************************************
**define which parts of the process to submit jobs for
local prep 0
local granular_art 0
local coverage 0
local incidence 0
local process_spectrum 1
*************************************


if `granular_art' == 1 {

di "Granular Coverage"
local files: dir "FILEPATH/" files "*_coverage.csv"

local counter 1
foreach file in `files' {
foreach scen in "better" "worse" {
local iso3 =  subinstr("`file'","_coverage.csv","",.)
**Don't need to pull in national aggregates for places we will be running subnationals
if !inlist("`iso3'","BRA","CHN","GBR","IND","JPN") & !inlist("`iso3'","KEN","MEX","MOZ","SAU","SWE","USA","ZAF","MDA") {
di "`iso3'" "`scen'"
di `counter'
local ++counter

! qsub `flag' -pe multi_slot 10 -N hiv_03_`iso3'_`scen' -hold_jid hiv_02_`iso3'_`scen' "`root'/code/r_shell.sh" "`root'/code/03_forecast_incidence.R"  "`fbd_version'"  "`iso3'" "`scen'"


}
}

}

}


if `process_spectrum' == 1 {
cap mkdir "FILEPATH/spectrum_mortality/"
local counter 1
use "FILEPATH/IHME_GDB_2015_LOCS_6.1.15.dta",clear
keep if location_type_id == 2
levelsof ihme ,l(isos)
foreach iso in "DOM" "CPV" "GNB" {
foreach scen in "reference" "better" "worse" {

! qsub `flag' -pe multi_slot 10 -N hiv_04_`iso'_`scen' "FILEPATH/r_shell.sh" "FILEPATH/04_scale_spectrum_output.R" "`fbd_version'"  "`iso'" "`scen'"


local ++counter
}
}
}


exit, clear STATA
Stop



