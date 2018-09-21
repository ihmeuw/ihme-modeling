** Purpose: Drop or adjust country years of data based on completeness from the demographics team

set more off, perm
if c(os) == "Unix" {
	set mem 10G
	set odbcmgr unixodbc
	global j "/home/j"
}
else if c(os) == "Windows" {
	set mem 800m
	global j "J:"
}

local lsvid 69

global source "`1'"

local testing 0
if `testing'==1 {
	global timestamp "2016_02_09"
	use "$j/WORK/03_cod/01_database/03_datasets/$source/data/final/07_merged_long.dta", clear
}

tempfile data
save `data', replace

import excel using "$j/WORK/03_cod/01_database/02_programs/compile/data/completeness_drops/completeness_adjustments.xlsx", sheet("drop") firstrow clear
keep ihme_loc_id year
gen drop = 1
gen subnat = 0
tempfile comp
save `comp', replace
import excel using "$j/WORK/03_cod/01_database/02_programs/compile/data/completeness_drops/completeness_adjustments.xlsx", sheet("nonrepresentative") firstrow clear
keep ihme_loc_id year
gen subnat = 1
gen drop = 0
append using `comp'

tempfile comp
save `comp', replace

odbc load, exec("SELECT location_id, ihme_loc_id FROM shared.location_hierarchy_history WHERE location_set_version_id=`lsvid'") strConnection clear
tempfile locs
save `locs', replace

merge 1:m ihme_loc_id using `comp', assert(1 3) keep(3) nogen
replace location_id = . if length(ihme_loc_id)==3
replace ihme_loc_id = substr(ihme_loc_id, 1, 3)
rename ihme_loc_id iso3

merge 1:m iso3 location_id year using `data', keep(2 3) nogen

** save drops, then drop & make things subnational that should be
export delimited using "$j/WORK/03_cod/01_database/02_programs/compile/explore/completeness_drops/${source}_drops.csv" if drop==1 & regexm(lower(source_type), "(vr)|(vital)"), replace
drop if drop==1 & regexm(lower(source_type), "(vr)|(vital)")
replace national = 0 if subnat==1 & regexm(lower(source_type), "(vr)|(vital)")
drop drop subnat

** DONE
