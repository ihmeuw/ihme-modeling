// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		USERNAME
// Description:	parallel code for applying long term probabilities to short-term incidence to get long-term incidence
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}
	if "`1'"=="" {
		local 1 FILEPATH
		local 2 FILEPATH
		local 3 DATE
		local 4 "05b"
		local 5 long_term_inc_to_raw_prev
		local 6 "FILEPATH"
		local 7 20
		local 8 2010
		local 9 2
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
	// sex
	local sex `9'
	// directory for external inputs
	local in_dir "`root_j_dir'/FILEPATH"
	// directory for output on the J drive
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on clustertmp
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	// directory for standard code files
	adopath + "FILEPATH"
	
	// write log if running in parallel and log is not already open
	log using "`out_dir'/FILEPATH.smcl", replace name(worker)
	
// SETTINGS
	** how many slots is this script being run on?
	local slots 4
	
// Filepaths
	local gbd_ado "FILEPATH"
	local diag_dir "FILEPATH"
	local old_date "DATE"
	
// Import functions
	adopath + "`code_dir'/ado"
	adopath + `gbd_ado'
	adopath + "FILEPATH"
	
	start_timer, dir("`diag_dir'") name("lt_probs_`location_id'_`year'_`sex'") slots(`slots')
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
	// set locals
	** do you want to save the draws of lt incidence, or just the summary statistics?
	local save_draws = 1
	
	** get the step number of the step with short term incidence numbers
	import excel using "`code_dir'/FILEPATH.xlsx", sheet("steps") firstrow clear
	preserve
	keep if name == "scaled_short_term_en_inc_by_platform"
	local last_step = step in 1
	local st_inc_dir "`root_tmp_dir'/FILEPATH"
	restore
	keep if name == "prob_long_term"
	local prev_step = step in 1
	local prob_dir "`root_tmp_dir'/FILEPATH"
		
** get levelsof inpatient/ncodes/ecodes to calculate
	** bring in the durations for this platform/ncode combination
		local success 0
	while !`success' {
		cap import delimited "`prob_dir'/FILEPATH.csv", delim(",") varnames(1) asdouble clear
		if !_rc local success 1
		else sleep 30000
	}
	
	rename draw_* prob_draw_*
	rename age_gr age
	preserve
	drop if n_code == "N1" | n_code == "N2" | n_code == "N3" | n_code == "N4" | n_code == "N5" | n_code == "N6" | n_code == "N7" 
	tempfile prob_draws
	save `prob_draws', replace
	restore
	// We are now handling probabilities at the n-code/age level, so we need to merge 100% long-term n-codes separately because they have no age (all amputations)
	keep if n_code == "N1" | n_code == "N2" | n_code == "N3" | n_code == "N4" | n_code == "N5" | n_code == "N6" | n_code == "N7" 
	keep if age == .
	forvalues i = 0/999 {
	replace prob_draw_`i' = 1 if prob_draw_`i' == .
	}
	tempfile all_lt
	save `all_lt', replace
	
	confirm file "`st_inc_dir'/FILEPATH.dta"
	use "`st_inc_dir'/FILEPATH.dta", clear
	// process draws
	rename draw* st_draw*
	rename ncode n_code
	// Merge all_lt probs
preserve
merge m:1 n_code inpatient using `all_lt', keep(3) nogen
tempfile all_lt_draws
save `all_lt_draws', replace
restore
	// Merge everything else
merge m:1 n_code age inpatient using `prob_draws', keep(3) nogen
append using `all_lt_draws'
	
	forvalues i=0/999 {
		generate draw_`i' = prob_draw_`i' * st_draw_`i'
		drop prob_draw_`i' st_draw_`i'
	}
	rename ecode e_code
	
	** drop out E/N/inp combinations that have zero long-term incidence
	egen double dropthisn=rowtotal(draw*)
	bysort e_code n_code inpatient : egen ensum = sum(dropthisn)
	drop if ensum==0
	// save draws in intermediate folder
	keep age draw* n_code e_code inpatient
	format draw* %16.0g
	if `save_draws'==1 {
		cap mkdir "`tmp_dir'/FILEPATH/"
		cap mkdir "`tmp_dir'/FILEPATH"
		cap mkdir "`tmp_dir'/FILEPATH"
		save "`tmp_dir'/FILEPATH.dta", replace
	}
// save summary in intermediate folder (stata files easier to append)
	fastrowmean draw_*, mean_var_name(mean_)
	fastpctile draw_*, pct(2.5 97.5) names(ll ul)
	egen meas_stdev = rowsd(draw_*)
	
	format mean ul ll meas_std %16.0g		

	keep age mean ul ll meas_stdev e_code n_code inpatient
	compress
		cap mkdir "`tmp_dir'/FILEPATH"	
		cap mkdir "`tmp_dir'/FILEPATH"	
		cap mkdir "`tmp_dir'/FILEPATH"
	save "`tmp_dir'/FILEPATH.dta", replace

	
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)
		
	// end the timer for this sub-step
		end_timer, dir("`diag_dir'") name("lt_probs_`location_id'_`year'_`sex'")
	
	// Close log
		log close worker
		erase "`out_dir'/FILEPATH.smcl"

		*end
