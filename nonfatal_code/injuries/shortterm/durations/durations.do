// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		Multiply treated and untreated durations by country-year-specific %-treated to get country-year-specific durations of short-term outcomes
// Author:		FILEPATH
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

	if "`1'"=="" {
		local 1 FILEPATH
		local 2 FILEPATH
		local 3 DATE
		local 4 "01d"
		local 5 durations
		local 6 FILEPATH
		local 7 178
		local 8 2000
	}
	// base directory on FILEPATH
	local root_j_dir `1'
	// base directory on FILEPATH
	local root_tmp_dir `2'
	// timestamp of current run (i.e. 2014_01_17)
	local date `3'
	// step number of this step (i.e. 01a)
	local step_num `4'
	// name of current step (i.e. first_step_name)
	local step_name `5'
    // directory where the code lives
    local code_dir `6'
    // iso3
	local location_id `7'
	// year
	local year `8'
	// directory for external inputs
	local in_dir "`root_j_dir'/FILEPATH"
	// directory for output on the FILEPATH
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on FILEPATH
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	// directory for standard code files
	adopath + "FILEPATH"
	
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// Settings
	set type double, perm

	local pct_file "`out_dir'/FILEPATH.dta"
	local dur_file "`out_dir'/FILEPATH.dta"

// update adopath
	adopath + "`code_dir'/ado"
	
// Start timer
	//start_timer, dir("`diag_dir'") name("`name'") slots(`slots')

// Load injury parameters
	load_params
	
// Merge on pct treated
	use "`dur_file'", clear
	gen location_id = `location_id'
	gen year_id = `year'
	merge m:1 location_id year_id using "`pct_file'", keep(match) nogen
	
// Generate final durations
	forvalues x = 0/$drawmax {
		replace treat_`x' = pct_treated_`x' * treat_`x' + (1-pct_treated_`x') * untreat_`x'
		drop untreat_`x'
		drop pct_treated_`x'
	}
	rename treat_* draw*
	
// Save draws
	drop location_id year
	sort_by_ncode ncode, other_sort(inpatient)
	export delimited "`tmp_dir'/FILEPATH.csv", replace
	
// Save summary
	fastrowmean draw*, mean_var_name("mean")
	fastpctile draw*, pct(2.5 97.5) names(ul ll)
	drop draw*
	export delimited "`tmp_dir'/FILEPATH.csv", replace
	