// Author: NAME
// Date: 6/22/15
// Purpose: Create a parallelized set of jobs by region to save country-specific inputs to Spectrum, 
// 			because we have too many iso3s and it takes too long to do it interactively 


// prep stata
	clear all
	set more off
	set mem 4g
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "ADDRESS"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "ADDRESS"
	}

** If all arguments are passed in:
	if "`3'" != "" {
		local region_id "`1'"
		local super_id "`2'"
		local archive "`3'"
		local date "`4'"
	}
	
** Otherwise display error message.
	else {
		noisily di in red "Missing arguments"
		error(999)
	}

// Prep data etc.
	adopath + "FILEPATH"
	get_locations
	keep if region_id == `region_id'
	levelsof ihme_loc_id, local(isos)

	local tmp_prep_file = "FILEPATH"

// Set filepaths
	local save_spectrum "FILEPATH"
	local archive_dir "FILEPATH" 
	local store_dir "`save_spectrum'/DisMod"
	local store_dir_HI "`save_spectrum'/DisMod_HI"
	local store_dir_BESTSSA "`save_spectrum'/DisMod_BestSSA"


** Save true region data
	use "`tmp_prep_file'", clear
	foreach i of local isos {
		di in red "Prepping `i' Regular"
		use "`tmp_prep_file'" if super == "`super_id'", clear	
		drop super
		outsheet using "`store_dir'/`i'_HIVonART.csv", delim(",") replace 
		if `archive' == 1 outsheet using "FILEPATH/`i'_HIVonART.csv", delim(",") replace 
	} 

	
** Save High-income counterfactual 
	foreach i of local isos {
		use "`tmp_prep_file'" if super == "high", clear	
		drop super
		outsheet using "`store_dir_HI'/`i'_HIVonART.csv", delim(",") replace 
		if `archive' == 1 outsheet using "FILEPATH/`i'_HIVonART.csv", delim(",") replace 
	}

	
** Save BEST SSA counterfactual 
	foreach i of local isos {
		use "`tmp_prep_file'" if super == "ssabest", clear	
		drop super
		outsheet using "`store_dir_BESTSSA'/`i'_HIVonART.csv", delim(",") replace 
		if `archive' == 1 outsheet using "FILEPATH/`i'_HIVonART.csv", delim(",") replace 
	}

	