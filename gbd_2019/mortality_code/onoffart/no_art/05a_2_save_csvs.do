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
	if "`1'" != "" {
		local region_id "`1'"
	}
	
** Otherwise display error message.
	else {
		noisily di in red "Missing arguments"
		error(999)
	}

	local comp_dir "FILEPATH"
	local progression_path "FILEPATH"
	local mortality_path "FILEPATH"

// Prep data etc.
	adopath + "FILEPATH"
	get_locations
	keep if region_id == `region_id'
	levelsof ihme_loc_id, local(isos)

// Output CSVs
	cd "`comp_dir'/FILEPATH"
	foreach t in mortality progression {
		use "`t'_compiled.dta", clear
		
		foreach i of local isos {
			di "`i'" 
			outsheet using "``t'_path'/`i'_`t'_par_draws.csv", replace comma names
		}
	}


