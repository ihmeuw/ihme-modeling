// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		USERNAME
// Description:	calculate proportion of long-term prevalence that is truly short-term due to overlap, and subtract from raw long-term prevalence

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
		local 4 "06b"
		local 5 long_term_final_prev_by_platform
		local 6 "FILEPATH"
		local 7 160
		local 8 2016
		local 9 1
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
	// directory for output on FILEPATH
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on FILEPATH
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	// directory for standard code files
	adopath + "$prefix/FILEPATH"
	
	// write log if running in parallel and log is not already open
	local log_file "`tmp_dir'/FILEPATH.smcl"
	log using "`log_file'", replace name(worker)
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// SETTINGS
	set type double, perm
	** how many slots is this script being run on?
	local slots 4
	** debugging?
	local debug 0

// Filepaths
	local gbd_ado "$prefix/FILEPATH"
	local diag_dir "`tmp_dir'/FILEPATH"
	local rundir "`root_tmp_dir'/FILEPATH"
	local draw_dir "`tmp_dir'/FILEPATH"
	local summ_dir "`tmp_dir'/FILEPATH"
	
	cap mkdir "`tmp_dir'/FILEPATH"
	cap mkdir "`draw_dir'"
	cap mkdir "`summ_dir'"
	
// Import functions
	adopath + "`code_dir'/ado"
	adopath + `gbd_ado'

// Start timer
	start_timer, dir("`diag_dir'") name("`location_id'_`year'_`sex'") slots(`slots')
	
// Load injury parameters
	load_params

// get file paths for the results for short_term_en_prev_yld_by_platform, prob_long_term long_term_inc_prev
	import excel "`code_dir'/_inj_steps.xlsx", firstrow clear
	preserve
	keep if name == "prob_long_term"
	local this_step=step in 1
	local long_term_prob_dir = "FILEPATH"
	restore, preserve
	keep if name == "scaled_short_term_en_prev_yld_by_platform"
	local this_step=step in 1
	local short_term_prev_dir = "`rundir'/FILEPATH"
	restore
	keep if name == "long_term_inc_to_raw_prev"
	local this_step=step in 1
	local long_term_prev_dir = "`rundir'/FILEPATH"


// calculate proportion of long-term prevalence that is actually short term (short term prevalence * probability of long-term outcome)

	** load short term prev
use "`short_term_prev_dir'/FILEPATH.dta", clear 
tostring age, replace force format(%12.3f)
destring age, replace force

local ecodes "inj_homicide_knife inj_homicide_gun inj_homicide_other inj_suicide_firearm inj_suicide_other inj_mech_other inj_mech_suffocate inj_mech_gun"
levelsof ncode, l(ncodes) clean
local platform 0 1

tempfile st_prev
save `st_prev'
	
	** load long-term prob
import delimited "`long_term_prob_dir'/FILEPATH.csv", asdouble clear
tostring age, replace force format(%12.3f)
destring age, replace force
rename draw* prob_draw*
	
	** merge and multiply (drop any 100% or 0% Long-term n-codes b/c we wont need to subtract these from total long-term prev)
merge 1:m age ncode inpatient using `st_prev', keep(match) nogen
forvalues x = 0/$drawmax {
	replace draw_`x' = draw_`x' * prob_draw_`x'
	drop prob_draw_`x'
}
rename draw* fake_long_draw*
tempfile fake_long_term
save `fake_long_term'
	
	
// Subtract duplicated short-term prev from long-term prev
	** import raw long-term prevalence
insheet using "`long_term_prev_dir'/FILEPATH.csv", comma names clear
tostring age, replace force format(%12.3f)
destring age, replace force
capture rename e_code ecode
capture rename n_code ncode
	
	** merge with double-counted prevalence and subtract
merge 1:1 age ecode ncode inpatient using `fake_long_term', keep(match master) nogen
// merge 1:1 age ecode ncode inpatient using `fake_long_term', assert(match master) nogen
// codebook inpatient if _merge == 2
// tab ncode if _merge == 1 & inpatient==1
// tab ecode if _merge == 1 & inpatient==1
forvalues x = 0/$drawmax {
replace draw_`x' = draw_`x' - fake_long_draw_`x' if fake_long_draw_`x' != .
drop fake_long_draw_`x'
}
	
	
// Save results
order ecode ncode inpatient age, first
// sort_by_ncode ncode, other_by(inpatient age)
sort ecode ncode inpatient age

// build in check to correct specific draws that are massive negative/positive numbers
forvalues i = 0/999 {
replace draw_`i' = . if draw_`i' < 0 | draw_`i' > 2
}
fastrowmean draw*, mean_var_name("mean")
forvalues i = 0/999 {
replace draw_`i' = mean if draw_`i' == .
}
drop mean
	

// delete outpatient poisoning and contusion
forvalues i = 0/999 {
replace draw_`i' = 0 if ncode == "N41" & inpatient == 0
}
forvalues i = 0/999 {
replace draw_`i' = 0 if ncode == "N44" & inpatient == 0
}

preserve 
	insheet using "$prefix/FILEPATH.csv", comma names clear
	gen remove = 1
	tempfile animal_remove
	save `animal_remove', replace
restore
merge m:1 ncode ecode inpatient using `animal_remove', keep(1 3) assert(1 3) nogen
forvalues i = 0/999 {
	replace draw_`i' = 0 if remove == 1
}
drop remove


// don't allow long-term prevalence of outpatient injuries from N48, N26, N11, N19, N43, N25, N23
forvalues i = 0/999 {
replace draw_`i' = 0 if (ncode == "N48" | ncode == "N26" | ncode == "N11" | ncode == "N19" | ncode == "N43" | ncode == "N25" | ncode == "N23") & inpatient == 0
}

forvalues i = 0/999 {
	replace draw_`i' = 0 if age < 1 & (ecode == "inj_war_execution" | ecode == "inj_disaster" | ecode == "inj_war_warterror")
}

	** draws
format draw* %16.0g
cap mkdir "`draw_dir'/FILEPATH"
save "`draw_dir'/FILEPATH.dta", replace
		
	** summary stats
fastrowmean draw*, mean_var_name("mean")
fastpctile draw*, pct(2.5 97.5) names(ll ul)
drop draw*
cap mkdir "`summ_dir'/FILEPATH"
save "`summ_dir'/FILEPATH.dta", replace


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)

// End timer
	end_timer, dir("`diag_dir'") name("`location_id'_`year'_`sex'")
	
// End log
	log close worker
	if !`debug' erase "`log_file'"
	
// write check file to indicate sub-step has finished
	file open finished using "`tmp_dir'/FILEPATH.txt", replace write
	file close finished
		
		*end
