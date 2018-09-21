// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		Multiply treated and untreated durations by country-year-specific %-treated to get country-year-specific durations of short-term outcomes
// Author:		USERNAME
// Date:		DATE

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "DIRECTORY"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "DIRECTORY"
	}

	forvalues i = 1/8 {
		di "Arg`i': ``i''"
	}

	if "`1'" == "" {
		local 1 = "FILEPATH"
		local 2 = "FILEPATH"
		local 3 = "DATE"
		local 4 = "01d"
		local 5 = "durations"
		local 6 = "FILEPATH"
		local 7 160
	}

	// base directory on FILEPATH 
	local root_j_dir `1'
	di "`root_j_dir'"
	// base directory on FILEPATH
	local root_tmp_dir `2'
	di "`root_tmp_dir'"
	// timestamp of current run (i.e. 2014_01_17)
	local date `3'
	di "`date'"
	// step number of this step (i.e. 01a)
	local step_num `4'
	di "`step_num'"
	// name of current step (i.e. first_step_name)
	local step_name `5'
	di "`step_name'"
    // directory where the code lives
    local code_dir `6'
    di "`code_dir'"
    // iso3
	local location_id `7'
	di "`location_id'"
	// directory for external inputs
	local in_dir "`root_j_dir'/FILEPATH"
	// directory for output on FILEPATH
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on FILEPATH
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	// directory for standard code files
	adopath + "FILEPATH"

	get_demographics, gbd_team("epi")
	global year_ids `r(year_ids)'
	global sex_ids `r(sex_ids)'
	global location_ids `r(location_ids)'
	global age_group_ids `r(age_group_ids)' 

	clear
	tempfile appended
	save `appended', emptyok 
	foreach year of global year_ids {
		insheet using "`tmp_dir'/FILEPATH.csv", comma names clear
		gen year_id = `year'
		append using `appended'
		save `appended', replace
	}

sort ncode inpatient year_id
	gen index_year = 1
	forvalues i = 1/4 {
		expand 2 if index_year == 1, gen(dup)
		replace index_year = 0 if dup == 1	
		replace year = year + `i' if dup == 1 
		drop dup
	}

	expand 2 if year_id == 2010, gen(dup)
	replace year_id = 2015 if dup == 1
	replace index_year = 0 if dup == 1
	drop dup

forvalue i = 0/999 {
	replace draw`i' = . if index_year == 0
	sort ncode inpatient year_id
	by ncode inpatient: ipolate draw`i' year_id, gen(draw`i'_ipo) epolate
	drop draw`i'
	rename draw`i'_ipo draw`i'
}

drop if year_id > 2016
drop index_year

// save draw files

cap mkdir "`tmp_dir'/FILEPATH"
cap mkdir "`tmp_dir'/FILEPATH"
tempfile all
save `all', replace
forvalues year = 1990/2016 {
	use `all' if year_id == `year', clear
	outsheet using "`tmp_dir'/FILEPATH.csv", comma names replace
}

// create summary files

use `all'

fastrowmean draw*, mean_var_name("mean")
fastpctile draw*, pct(2.5 97.5) names(ul ll)
drop draw*

tempfile all
save `all', replace

cap mkdir "`tmp_dir'/FILEPATH"
cap mkdir "`tmp_dir'/FILEPATH"

forvalues year = 1990/2016 {
	use `all' if year_id == `year', clear
	outsheet using "`tmp_dir'/FILEPATH.csv", comma names replace
}

// END
