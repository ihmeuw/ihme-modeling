// APPEND THE DATA USED FOR THE E-N MATRICES THAT WAS CLEANED IN THE 00_CLEAN STEPS AND PREPPED IN THE 01_PREP STEPS

	clear all
	set more off
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		set odbcmgr unixodbc
	}
	
// Import macros
	local check 1
	if `check' == 1 {
		local 1 FILEPATH
		local 2 "FILEPATH"
		local 3 "FILEPATH"
		local 4 FILEPATH
	}
	global prefix `1'
	local prepped_dir `2'
	local appended_dir `3'
	local log_dir `4'
	
adopath + "FILEPATH"
adopath + "FILEPATH"
// load_params

// Write log
	cap log using "`log_dir'/FILEPATH.smcl", replace
	if !_rc local close_log 1
	else local close_log 0
	
// Import datasets
	local datasets: dir "`prepped_dir'" files "prepped_*"
	local datasets = subinstr(`"`datasets'"',`"""',"",.)
	
	tempfile appended
	foreach ds of local datasets {
		import delimited `prepped_dir'/`ds', delim(",") clear
		gen ds = "`ds'"
		cap confirm file `appended'
		if !_rc append using `appended'
		save `appended', replace
	}

foreach ncode of global inp_only_ncodes	{
drop if n_code == "`ncode'" & ds == "FILEPATH.csv"
}
drop if n_code == "N14" & ds == "FILEPATH.csv" & e_code == "inj_fires"

	save `appended', replace
	
	// Merge on super region
	clear
	get_location_metadata, location_set_id(35)
	rename ihme_loc_id iso3
	keep iso3 super_region_id super_region_name
	gen high_income=0
	replace high_income=1 if super_region_name=="High-income"
	keep iso3 high_income 
	tempfile iso3_to_sr_map
	save `iso3_to_sr_map', replace
	
	** merge this on
	merge 1:m iso3 using `appended', assert(match master)
	keep if _m == 3
	drop _m
	
	// For diagnosing countries with high spinal lesions
	
	drop iso3 location_id year ds
	drop if n_code==""
// Group by desired level and pivot so that N-codes are columns
	collapse (sum) cases, by(e_code n_code high_income age sex inpatient)
	
	reshape wide cases, i(e_code high_income age sex inpatient) j(n_code) string
	** missing values after reshape are really 0's
	foreach var of varlist cases* {
		replace `var' = 0 if `var' == .
	}
	rename cases* *
	
// Create e-code totals column
	egen totals = rowtotal(N*)
	
// drop medical misadventure e-codes b/c we will map them 100% to medical misadventure N-code
	drop if e_code == "inj_medical"

// Save
	compress
	export delimited using "`appended_dir'/FILEPATH.csv", delim(",") replace
	
	if `close_log' log close
	