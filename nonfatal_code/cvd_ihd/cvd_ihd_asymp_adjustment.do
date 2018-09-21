// Run this AFTER MI Survivors DisMod model has been run off of uploaded data. 
// This adjusts the MI prevalence results to not include people who survive MI and end up getting angina or heart failure
// Specific proportions from literature for age and sex-specific, and low vs high-income countries.
// Specific proportions for HF and for angina
// EDIT FOR GBD 2016 - switch to 15755 for DisMod model; save results to 3233
** *****************************************************************************
** Setup
** *****************************************************************************

clear all
set more off
cap log close
set mem 5g

if c(os) == "Unix" {
	global prefix "FILEPATH"
	set odbcmgr unixodbc
}
else if c(os) == "Windows" {
	global prefix "FILEPATH"
}
adopath + "FILEPATH"

// PULL IN LOCATION_ID FROM BASH COMMAND
	local location "`1'"
	
	capture log close
	log using FILEPATH/log_asymp_`location', replace

	local prop_dir "FILEPATH"
	local out_dir "FILEPATH"

// Adjust for angina, using proportions based off of literature search
insheet using "`prop_dir'/angina_prop_postMI.csv", comma clear
replace angina_prop = 0 if angina_prop == . // Ages under 20
keep age_group_id angina_prop
duplicates drop
tempfile prop_file
save `prop_file'

get_draws, gbd_id_field(modelable_entity_id) gbd_id(15755) source(dismod) location_ids(`location') measure_ids(5) clear
merge m:1 age_group_id using `prop_file', keep(3) nogen

qui forvalues num = 0/999 {
	gen angina_adjust = draw_`num' * angina_prop
	replace draw_`num' = draw_`num' - angina_adjust
	drop *_adjust
}

drop angina*
tempfile master
save `master', replace

// Assume that all HF due to IHD comes after MI, and subtract it from post-MI survivors
clear all
tempfile hf_master
save `hf_master', emptyok
get_draws, gbd_id_field(modelable_entity_id) gbd_id(9567) source(dismod) location_ids(`location') measure_ids(5) clear

forvalues num = 0/999 {
	replace draw_`num' = draw_`num' * -1
	}

append using `master' 
fastcollapse draw_*, type(sum) by(age_group_id sex_id year_id)

forvalues num = 0/999 {
	qui replace draw_`num' = 0 if age_group_id <= 11 // Hard-coding an age restriction
	qui count if draw_`num' < 0
	if r(N) > 0 & r(N) < 500 {
		di in red "count is " r(N)
		di in red "Draw `num' has a value less than zero!"
		qui levelsof age_group_id if draw_`num' < 0, local(ages)
		qui foreach age in `ages' {
			di in red "`age'"
			sum draw_`num' if age == `age'
		}
		replace draw_`num' = 0 if draw_`num' < 0
	}
	
	if r(N) > 500 {
		di in red r(N)
		di in red "Draw `num' has TONS OF VALUES less than zero!"
		levelsof age if draw_`num' < 0, local(ages)
		foreach age in `ages' {
			di in red "`age'"
			sum draw_`num' if age == `age'
		}
		BREAK
	}
}

forvalues num = 0/999 {
	replace draw_`num' = 0 if draw_`num'<0
	}

local years 1990 1995 2000 2005 2010 2016
local sexes 1 2 

foreach year of local years {
			foreach sex of local sexes {
				outsheet age_group_id draw_* if sex_id==`sex' & year_id==`year' using "`out_dir'/5_`location'_`year'_`sex'.csv", comma replace
			}
		}
log close
