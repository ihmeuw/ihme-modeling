// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
// Purpose:		Computes MDR-TB proportions weighted for the proportions of new and previously treated cases in the population
// Author:		USERNAME
// Edited:      USERNAME
// Description:	Compute proportion of new and previously treated cases in the population
//				Weight MDR new proportions
//				Weight MDR retreated proportions
//				Aggregate proportions to derive weighted MDR-TB proportions 
// Variables:	decomp_step, draw_dir
// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
         
**********************************************************************************************************************
** ESTABLISH SETTINGS FOR SCRIPT
**********************************************************************************************************************		 
		 
// Load settings

	// Clear memory and establish settings
	clear all
	set more off
	set scheme s1color

	// Define focal drives
	if c(os) == "Unix" {
		global prefix "ADDRESS"
		local function_prefix "ADDRESS"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "ADDRESS"
		local function_prefix "ADDRESS"
	}

	// Close any open log file
	cap log close
	
// Load helper objects and directories
	
	// locals
	local decomp_step step4
	local draw_dir "FILEPATH"

**********************************************************************************************************************
** STEP 1: PREP RETREATED DRAWS FOR COMPUTATIONS
**********************************************************************************************************************

	// Read in the proportions of relapse+retreated cases among all TB cases from ST-GPR
	use "`draw_dir'/tb_rel_ret_prop_draws_`decomp_step'.dta", clear

	// Cap draws
	forvalues i=0/999 {
		replace draw_`i'=0.99 if draw_`i'>0.99
	}

	// Rename draws
	forvalues i=0/999 {
		rename draw_`i' ret_prop_`i'
	}

	// Save tempfile
	tempfile ret_prop
	save `ret_prop', replace

	// Compute proportion
	forvalues i=0/999 {
		gen new_prop_`i'=1-ret_prop_`i'
	}

	// Clean
	drop ret_prop_*
	tempfile new_prop
	save `new_prop', replace

**********************************************************************************************************************
** STEP 2: PREP MDR NEW DRAWS FOR COMPUTATIONS
**********************************************************************************************************************

	// Bring in MDR new draws
	use "`draw_dir'/mdr_new_draws_`decomp_step'.dta", clear

	// Use national proportions for UKR subnationals
	preserve
		keep if location_id==63
		replace location_id=44934
		tempfile tmp_1
		save `tmp_1', replace
		replace location_id=44939
		tempfile tmp_2
		save `tmp_2', replace
		replace location_id=50559
		append using `tmp_1'
		append using `tmp_2'
		tempfile ukr_sub
		save `ukr_sub', replace
	restore

	// Append national proportion
	drop if inlist(location_id, 44934, 44939, 50559)
	append using `ukr_sub'

	// Hybridize two models to fix the poor fit for Macao and Hongkong
	drop if inlist(location_id, 354, 361)
	append using "`draw_dir'/mdr_new_draws_354_361_`decomp_step'.dta"

	// Cap draws
	forvalues i=0/999 {
		replace draw_`i'=0.99 if draw_`i'>0.99
	}

	// Merge on proportions of new TB cases
	merge m:1 location_id year_id age_group_id sex_id using `new_prop', keep(3) nogen

	// Weight MDR proportions by proportion of new cases
	forvalues i=0/999 {
		gen mdr_prop_wt_`i'=draw_`i'*new_prop_`i'
	}

	// Clean
	drop new_prop_* draw_*
	tempfile new_wt
	save `new_wt', replace

**********************************************************************************************************************
** STEP 3: PREP MDR RET DRAWS FOR COMPUTATIONS
**********************************************************************************************************************

	// Load in mdr_ret draws
	use "`draw_dir'/mdr_ret_draws_`decomp_step'.dta", clear

	// Use national proportions for UKR subnationals
	preserve
		keep if location_id==63
		replace location_id=44934
		tempfile tmp_1
		save `tmp_1', replace
		replace location_id=44939
		tempfile tmp_2
		save `tmp_2', replace
		replace location_id=50559
		append using `tmp_1'
		append using `tmp_2'
		tempfile ukr_sub
		save `ukr_sub', replace
	restore

	// Append proportions
	drop if inlist(location_id, 44934, 44939, 50559)
	append using `ukr_sub'

	// Hybridize two models to fix the poor fit for Macao, Hongkong, SWZ, and MOZ
	drop if inlist(location_id, 184, 197, 354, 361)
	append using "`draw_dir'/mdr_ret_draws_354_361_197_184_`decomp_step'.dta"

	// Cap draws
	forvalues i=0/999 {
		replace draw_`i'=0.99 if draw_`i'>0.99
	}

	// Merge on proportions of retreated TB cases
	merge m:1 location_id year_id age_group_id sex_id using `ret_prop', keep(3) nogen

	// Weight MDR draws by proportion of retreated cases
	forvalues i=0/999 {
		gen mdr_prop_wt_`i'=draw_`i'*ret_prop_`i'
	}

	// Clean
	drop ret_prop_* draw_*
	
**********************************************************************************************************************
** STEP 4: COMPUTE WEIGHTED AVERAGE MDR PROPORTIONS
**********************************************************************************************************************

	// Append weighted MDR new cases
	append using `new_wt'

	// Sum the draws to have a weighted average
	collapse (sum) mdr_prop_wt_*, by(location_id year_id age_group_id sex_id) fast

	// Rename draws
	forvalues i=0/999 {
		rename mdr_prop_wt_`i' draw_`i'
	}

	// Output the weighted draws	
	save "`draw_dir'/mdr_draws_`decomp_step'.dta", replace

	// Compute summaries
	egen mean=rowmean(draw_*)
	egen upper=rowpctile(draw_*), p(97.5)
	egen lower=rowpctile(draw_*), p(2.5)

	// Clean
	drop draw_*
