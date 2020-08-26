// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
// Purpose:		Computes MDR-TB proportions disaggregated by HIV status
// Author:		USERNAME
// Edited:      USERNAME
// Description:	Prep TB no-HIV, HIVTB, and TB all-forms results
//				Compute TB no-HIV MDR proportion 
//				Compute HIVTB MDR proportion
// Variables:	decomp_step, nf_decomp_step, draw_dir
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
	local nf_decomp_step step2	
	local stgpr_dir "FILEPATH"
	
**********************************************************************************************************************
** STEP 1: GATHER ALL THE INPUTS - HIVTB, TB NO-HIV, ALL TB
**********************************************************************************************************************

	// Pull TB all forms
	clear all
	adopath + "`function_prefix'/FILEPATH"
	get_model_results, gbd_team("epi") gbd_id(9806) decomp_step(`nf_decomp_step') clear

	// Subset data
	keep if measure_id==6
	keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235

	// Clean data
	duplicates drop location_id year_id age_group_id sex_id measure_id, force
	drop lower upper
	rename mean tb_inc

	// Save tempfile
	tempfile tb
	save `tb', replace

	// Pull populations	
	clear all
	adopath + "`function_prefix'/FILEPATH"
	get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") decomp_step(`decomp_step') clear

	// Save tempfile
	tempfile pop_all
	save `pop_all', replace
			
	// Pull HIV-TB
	get_model_results, gbd_team("epi") gbd_id(1176) decomp_step(`nf_decomp_step') clear

	// Subset data
	keep if measure_id==6
	keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235

	// Clean data
	duplicates drop location_id year_id age_group_id sex_id measure_id, force
	drop lower upper
	rename mean hiv_tb_inc

	// Save tempfile
	tempfile hiv_tb
	save `hiv_tb', replace

	// get TB no-HIV
	get_model_results, gbd_team("epi") gbd_id(9969) decomp_step(`nf_decomp_step') clear

	// Subset data
	keep if measure_id==6
	keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235

	// Clean data
	duplicates drop location_id year_id age_group_id sex_id measure_id, force
	drop lower upper
	rename mean tbnoHIV_inc

	// Save tempfile
	tempfile tb_noHIV
	save `tb_noHIV', replace

**********************************************************************************************************************
** STEP 2: EXTRAPOLATE AND INTERPOLATE FOR REMAINING YEARS
**********************************************************************************************************************

	// Merge all data and interpolate
	use `tb', clear
	merge 1:1 location_id year_id age_group_id sex_id using `hiv_tb', keep(3)nogen
	merge 1:1 location_id year_id age_group_id sex_id using `tb_noHIV', keep(3)nogen

	// Prepare to project back to 1980
	expand 2 if year_id==1990, gen(exp)
	replace year=1980 if exp==1

	// Drop exp variable
	drop exp 

	// Interpolate / extrapolate for years for which we don't have results
	egen panel = group(location_id age_group_id sex_id)
		tsset panel year_id
		tsfill, full
		bysort panel: egen pansex = max(sex_id)
		bysort panel: egen panage = max(age_group_id)
		replace sex_id = pansex
		drop pansex
		replace age_group_id=panage
	drop panage

	// Run interpolation
	foreach metric in "tb_inc" "hiv_tb_inc" "tbnoHIV_inc" {
		bysort panel: ipolate `metric' year, gen(`metric'_new) epolate
	}

	// Panels
	bysort panel: replace location_id = location_id[_n-1] if location_id==.
	bysort panel: replace age_group_id = age_group_id[_n-1] if age_group_id==.
	bysort panel: replace sex_id = sex_id[_n-1] if sex_id==.

	// Merge on population
	merge 1:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(population) keep(3)nogen

	// Output file
	save "FILEPATH/tb_hivtb_tbnohiv.dta", replace

**********************************************************************************************************************
** STEP 3: COMPUTE MDR PROPORTIONS FOR TB NO-HIV AND HIVTB
**********************************************************************************************************************

	// Pull in model results
	use "FILEPATH/tb_hivtb_tbnohiv.dta", clear

	// Clean column names
	replace tb_inc = tb_inc_new
	replace hiv_tb_inc = hiv_tb_inc_new
	replace tbnoHIV_inc = tbnoHIV_inc_new
	drop *_new

	// Generate ratio of HIVTB : TB no-HIV
	gen ratio=hiv_tb_inc/tbnoHIV_inc 
				
	// Merge in MDR draws
	merge m:1 location_id year_id using "`stgpr_dir'/mdr_draws_`decomp_step'.dta", keep(3)nogen

	/* gen rr=1.22882 */

	// Compute MDR-TB from TB all forms at 1000 draw level
	forvalues x = 0/999 {
		gen mdr_all_`x' = tb_inc*population*draw_`x'
	}

	// Clean
	drop draw_*

	// Merge on HIV RR - risk of MDR-TB associated with HIV infection
	gen acause="tb_drug"
	merge m:1 acause using "FILEPATH/hiv_mdr_RR_draws.dta", keep(3)nogen

	// Compute HIVTB MDR cases: hiv_mdr=(rr/(tbnoHIV/hiv_tb)*(all_tb*MDR_prop)/(rr/(tbnoHIV/hiv_tb)+1)
	forvalues x = 0/999 {
		gen tb_nohiv_mdr_cases_`x' = mdr_all_`x'/(1+(rr_`x'*ratio))
	}

	// Clean
	drop mdr_all_*					

	// Compute TB no-HIV cases
	gen tb_noHIV_cases= tbnoHIV_inc*population

	// Compute MDR proportion among TB no-HIV cases
	forvalues x = 0/999 {
		gen tb_nohiv_mdr_prop_`x' = tb_nohiv_mdr_cases_`x'/tb_noHIV_cases
	}

	// Clean
	keep location_id year_id age_group_id sex_id tb_nohiv_mdr_prop_* rr_*
	tempfile tb_nohiv_mdr
	save `tb_nohiv_mdr', replace

	// Drop column
	drop rr_*

	// Output MDR proportions
	save "FILEPATH/tb_nohiv_mdr_prop_draws.dta", replace

	// Get HIVTB
	use `tb_nohiv_mdr', clear

	// Compute HIVTB mdr prop
	forvalues x = 0/999 {
		gen hiv_mdr_prop_`x' = tb_nohiv_mdr_prop_`x'*rr_`x'
	}

	// Save
	keep location_id year_id age_group_id sex_id hiv_mdr_prop_*
	save "FILEPATH/hiv_mdr_prop_draws.dta", replace
			
