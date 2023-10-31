// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
// Purpose:		Incoporates retreated cases to disaggregated HIVTB child causes
// Author:		USERNAME
// Edited:      USERNAME
// Description:	Computes MDR-HIVTB incident HIVTB cases 
//				Disaggregates MDR-HIVTB into XDR-HIVTB and MDR-HIVTB without XDR-HIVTB
// 				Subtraction to compute drug-susceptible HIVTB
// Variables:	acause, version, model_version_id, decomp_step
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
	local acause tb
	local model_version_id dismod_478688_ret // me_id=9422 (dismod)
	local decomp_step step4

**********************************************************************************************************************
** STEP 1: COMPUTE MDR-HIVTB ALL BY APPLYING PROPORTIONS
**********************************************************************************************************************	

	// Get population
	clear all
	adopath + "FILEPATH"
	get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") decomp_step(`decomp_step') gbd_round_id(6) clear
	rename population mean_pop

	// Save tempfile for populations 
	tempfile pop_all
	save `pop_all', replace

	// Pull HIV-TB draws
	use FILEPATH/HIVTB_inc_cases_`model_version_id'_capped.dta, clear
	keep location_id year_id age_group_id sex_id draw_*

	// Merge MDR proportions
	merge m:1 location_id year_id age_group_id sex_id using "FILEPATH/hiv_mdr_prop_draws.dta", keep(3)nogen

	// Apply MDR proportions
	forvalues i = 0/999 {
		replace draw_`i'=draw_`i'*hiv_mdr_prop_`i'
		replace draw_`i'=0 if draw_`i'<0
	}

	// Clean
	drop hiv_mdr_prop_*

	// Rename draws
	forvalues i = 0/999 {
		rename draw_`i' mdrtb_`i'
	}

	// Save tempfile
	tempfile mdrtb
	save `mdrtb', replace

**********************************************************************************************************************
** STEP 2: COMPUTE XDR-HIVTB
**********************************************************************************************************************	

	// get SR data
	use "FILEPATH/locations_22.dta", clear
	keep location_id super_region_name

	// Save tempfile
	tempfile sr
	save `sr', replace

	// Pull MDR-HIVTB
	use `mdrtb', clear
	merge m:1 location_id using `sr', keep(3)nogen

	// Merge XDR proportions
	merge m:1 super_region_name year_id using "FILEPATH/xdr_prop_GBD2019.dta", keep(3)nogen

	// calculate XDR-HIVTB
	forvalues i=0/999 {
		gen draw_`i'=mdrtb_`i'*xdr_prop
		replace draw_`i'=0 if draw_`i'<0
	}

	// Save draws			
	keep location_id year_id age_group_id sex_id draw_*

	// Zero out before 1992 and below
	preserve 
	keep if year<1993
	forvalues i=0/999 {
		replace draw_`i'=0 
	}
	tempfile zero
	save `zero', replace

	// Append the zeros
	restore
	drop if year<1993
	append using `zero'

	// Clean
	gen modelable_entity_id=20271

	// Save tempfile
	tempfile xdr
	save `xdr', replace

**********************************************************************************************************************
** STEP 3: COMPUTE MDR-HIVTB WITHOUT XDR-HIVTB
**********************************************************************************************************************	

	// Rename draws
	forvalues i = 0/999 {
		rename draw_`i' xdr_`i'
	}

	// Merge on MDR-HIVTB
	merge 1:1 location_id year_id age_group_id sex using `mdrtb', keep(3) nogen 

	// Subtract XDR-HIVTB
	forvalues i=0/999 {
		gen draw_`i'=mdrtb_`i'-xdr_`i'
		replace draw_`i'=0 if draw_`i'<0
	}

	// Clean data
	keep location_id year_id age_group_id sex_id draw_*
	gen modelable_entity_id=20270


	// Merge pops to convert MDR cases into rate
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Loop through draws and calculate rate
	forvalues i=0/999 {
		replace draw_`i'=draw_`i'/mean_pop
		replace draw_`i'=0 if draw_`i'<0
	}

	// Clean
	drop mean_pop
	gen measure_id=6

	// Save MDR-HIVTB
	save "FILEPATH/mdr_hivtb_inc_`model_version_id'.dta", replace

	// Convert XDR cases into rates
	use `xdr', clear
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Loop through draws and calculate rate
	forvalues i=0/999 {
		replace draw_`i'=draw_`i'/mean_pop
		replace draw_`i'=0 if draw_`i'<0
	}

	// Clean
	drop mean_pop
	gen measure_id=6

	// Save XDR-HIVTB
	save "FILEPATH/xdr_hivtb_inc_`model_version_id'.dta", replace

