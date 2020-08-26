/// Append all cholera PAF files in preparation for save_results ///
// do "/filepath/04_create_cholera_paf_csvs.do"

global j "filepath"
set more off
clear all

// All of the cholera draws are saved separately, we need to merge them all //
// This is done in a loop and in parts to save memory. //
local multiplier 0 1 2 3 4
foreach m in `multiplier' {
	clear all
	local first = `m'*200 + 1
	set obs 200
	gen values = _n + 200*`m'
	levelsof values, local(l)
	di "filepath/draws`first'.dta"
	use "filepath/draws_`first'.dta", clear
	foreach i in `l' {
		qui merge 1:1 location_id age_group_id sex_id year_id using "filepath/draws_`i'.dta", force nogen
		di in red "`i'"
	}
	save "filepath/part_`m'_draws.dta", replace
}
use "filepath/part_0_draws.dta", clear
local newdraw 1 2 3 4
foreach m in `newdraw' {
	qui merge 1:1 location_id age_group_id sex_id year_id using "filepath/part_`m'_draws.dta", force nogen
}
saveold "filepath/decomp4_draws.dta", replace

// We now have our master draw file, loop through to save a single CSV by location //
use "filepath/decomp4_draws.dta", clear
replace age_group_id = 235 if age_group_id == 33

levelsof location_id, local(wheres)
local sexes 1 2
local years 1990 1995 2000 2005 2010 2017
keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)

keep location_id year_id age_group_id sex_id morb*
forval i = 1/1000 {
	local j = `i' - 1
	rename morbidity_`i' paf_`j'
}
gen cause_id = 302
save "/filepath/morbidity.dta", replace
foreach www of local wheres {
	di "`www' Morbidity"
		sort year_id age_group_id sex_id
																				  
			outsheet age_group_id location_id year_id sex_id cause_id paf_* using "/filepath/paf_yld_`www'.csv" if location_id == `www', comma replace
}

use "/filepath/decomp4_draws.dta", clear
replace age_group_id = 235 if age_group_id == 33

keep location_id year_id age_group_id sex_id mort*
forval i = 1/1000 {
	local j = `i' - 1
	rename mortality_`i' paf_`j'
}
gen cause_id = 302
save "/filepath/mortality.dta", replace
foreach www of local wheres {
	di "`www' Mortality"
	sort year_id age_group_id sex_id
			outsheet age_group_id location_id year_id sex_id cause_id paf_* using "/filepath/paf_yll_`www'.csv" if location_id == `www', comma replace 
}

// Finished! //
