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
		local 7 160
		local 8 1
		local 9 inp
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
	// sex
	local sex `8'
	// platform
	local platform `9'
	// directory for external inputs
	local in_dir "`root_j_dir'/FILEPATH"
	// directory for output on the FILEPATH drive
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on FILEPATH
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	// directory for standard code files
	adopath + "FILEPATH"
	
	// write log if running in parallel and log is not already open
	log using "`out_dir'/FILEPATH.smcl", replace name(step_worker)
	
// SETTINGS
	local slots 4

// Filepaths
	local diag_dir "`out_dir'/FILEPATH"
	
// Import functions
	adopath + "`code_dir'/ado"
	
	start_timer, dir("`diag_dir'") name("lt_probs_shock_`location_id'_`platform'_`sex'") slots(`slots')
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
	
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
	** loop over platforms

* Load map to age_group_id
	insheet using "`code_dir'/FILEPATH.csv", comma names clear
	rename age_start age
	tostring age, replace force format(%12.3f)
	destring age, replace force
	tempfile age_ids 
	save `age_ids', replace

		
** get levelsof inpatient/ncodes/ecodes to calculate
	** bring in the durations for this platform/ncode combination
	import delimited "`prob_dir'/FILEPATH.csv", delim(",") varnames(1) asdouble clear
	rename draw_* prob_draw_*
	rename age_gr age
	preserve
	drop if n_code == "N1" | n_code == "N2" | n_code == "N3" | n_code == "N4" | n_code == "N5" | n_code == "N6" | n_code == "N7" 
	tempfile prob_draws
	save `prob_draws', replace
	restore
	// Copy probabilities for ages <1, ages > 80
	preserve
	keep if age == 0
	gen merge_age = 1 if age == 0
	tempfile special_ages
	save `special_ages', replace
	restore
	// We are now handling probabilities at the n-code/age level, so we need to merge 100% long-term n-codes separately because they have no age (all amputations)
	keep if n_code == "N1" | n_code == "N2" | n_code == "N3" | n_code == "N4" | n_code == "N5" | n_code == "N6" | n_code == "N7" 
	keep if age == .
	forvalues i = 0/999 {
	replace prob_draw_`i' = 1 if prob_draw_`i' == . | prob_draw_`i' == 0
	}
	tempfile all_lt
	save `all_lt', replace
	
	confirm file "`st_inc_dir'/FILEPATH.dta"
	use "`st_inc_dir'/FILEPATH.dta", clear
	// process draws
	rename draw* st_draw*
	rename ncode n_code
	preserve
	merge m:1 n_code inpatient using `all_lt', keep(3) nogen
	tempfile all_lt_draws
	save `all_lt_draws', replace
	restore
	preserve
	gen merge_age = 1 if (age < 1 & age != 0)
	merge m:1 n_code inpatient merge_age using `special_ages', keep(3) nogen
	drop if n_code == "N1" | n_code == "N2" | n_code == "N3" | n_code == "N4" | n_code == "N5" | n_code == "N6" | n_code == "N7" 
	tempfile all_special_ages
	save `all_special_ages', replace
	restore
		// Merge everything else
	merge m:1 n_code age inpatient using `prob_draws', keep(3) nogen
	append using `all_lt_draws' `all_special_ages'

	forvalues i=0/999 {
		generate draw_`i' = prob_draw_`i' * st_draw_`i'
		drop prob_draw_`i' st_draw_`i'
	}
	** drop out E/N/inp combinations that have zero long-term incidence
	egen double dropthisn=rowtotal(draw*)
	drop if dropthisn==0
	levelsof ecode, local(ecodes)
	
	// Convert to age_group_id
	tostring age, replace force format(%12.3f)
	destring age, replace force

	merge m:1 age using `age_ids', keep(3) nogen
	drop age

	tempfile all
	save `all', replace
	

	if "`platform'"=="inp" {
		local platnum=1
	}	
	if "`platform'"=="otp" {
		local platnum=0
	}	

get_location_metadata, location_set_id(35) clear
keep if location_id == `location_id'
local iso3 = [ihme_loc_id]
if `sex' == 1 {
local sex_string male
}
if `sex' == 2 {
local sex_string female
}
	
	** save the draws in a format for ODE solver 
	foreach e of local ecodes {
	
		use if ecode=="`e'" & inpatient == `platnum' using `all', clear		
		levelsof n_code, local(ncodes)
		levelsof year, local(years)
		
		foreach n of local ncodes {
			
			foreach year of local years {
				di "Saving `ecode' and `platform' and `ncode' and `year'"
				use if ecode == "`e'" & inpatient==`platnum' & n_code=="`n'" & year==`year' using `all', clear
				keep age_group_id draw*
				format draw* %16.0g
			
				outsheet using "`tmp_dir'/FILEPATH.csv", comma names replace
				
				fastrowmean draw*, mean_var_name("mean")
				fastpctile draw*, pct(2.5 97.5) names(ll ul)
				drop draw*
				format mean ul ll %16.0g
				order age mean ll ul

				outsheet using "`tmp_dir'/FILEPATH.csv", comma names replace

			}
		}
	}

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)
		
	// end the timer for this sub-step
		end_timer, dir("`diag_dir'") name("lt_probs_shock_`location_id'_`platform'_`sex'")
	
	// Close log
		log close step_worker
		erase "`out_dir'/FILEPATH.smcl"
		