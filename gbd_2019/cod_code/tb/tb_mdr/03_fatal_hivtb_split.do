// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
// Purpose:		Disaggregate HIV-TB deaths into drug resistance status
// Author:		USERNAME
// Edited:      USERNAME
// Description:	computes MDR-HIVTB envelope by applying MDR PAF to HIVTB envelope
//				subtracts MDR-HIVTB deaths from HIVTB envelope to compute DS-HIVTB deaths
//				subtracts XDR-HIVTB from MDR-HIVTB envelope to derive MDR-HIVTB (without XDR-TB)
// 				saves results to CoD database
// Variables:	acause, custom_version, decomp_step
// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************

**********************************************************************************************************************
** ESTABLISH SETTINGS FOR SCRIPT
**********************************************************************************************************************

// Load settings

	// Clear memory and establish settings
	clear all
	set mem 5G
	set maxvar 32000
	set more off

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

// Load helper objects and folders
		
	// Load locals
	local acause hiv_tb
	local custom_version v1.8
	local decomp_step step4
				
	// Make folders on cluster
	capture mkdir "FILEPATH/`custom_version'"
	capture mkdir "FILEPATH/xdr/`custom_version'"
	capture mkdir "FILEPATH/mdr/`custom_version'"
	capture mkdir "FILEPATH/drug_susceptible/`custom_version'"

**********************************************************************************************************************
** COMPUTE MDR-HIVTB ENVELOPE USING HIVTB ENVELOPE AND PAF
**********************************************************************************************************************

	// Load in HIVTB mortality envelope 
	use FILEPATH/hivtb_capped_`custom_version'.dta, clear
	duplicates drop location_id year_id age_group_id sex_id, force
	tempfile hivtb
	save `hivtb', replace

	// Merge MDR PAF
	merge 1:1 location_id year_id age_group_id sex_id using "FILEPATH/par_hivtb_mdr_draws.dta", keep(3)nogen

	// Compute MDR-HIVTB deaths
	forvalues i = 0/999 {
		replace draw_`i'=draw_`i'*PAR_based_on_mean_rr_`i'
	}

	// Clean
	drop PAR_based_on_mean_rr_*

	// Rename draws
	forvalues i = 0/999 {
		rename draw_`i' mdrtb_`i'
	}

	// Save tempfile
	tempfile mdrtb
	save `mdrtb', replace

**********************************************************************************************************************
** COMPUTE DRUG-SENSITIVE HIVTB
**********************************************************************************************************************

	// Merge MDR-HIVTB envelope to HIVTB envelope
	use `hivtb', clear
	merge 1:1 location_id year_id age_group_id sex_id using `mdrtb', keep(3) nogen 

	// negative values found for DS HIV-TB, needs to investigate
	// loop through draws and subtract mdr_hivtb from hivtb 
	forvalues i=0/999 {
		replace draw_`i'=draw_`i'-mdrtb_`i'
		replace draw_`i'=0 if draw_`i'==.
		replace draw_`i'=0 if draw_`i'<0
	}

	// Clean
	drop mdr*			
	replace cause_id=948	

	// Save as csvs
	levelsof(location_id), local(ids) clean
	foreach location_id of local ids {	
			qui outsheet if location_id==`location_id' using "FILEPATH/`location_id'.csv", comma replace
	}

**********************************************************************************************************************
** COMPUTE XDR-HIVTB
**********************************************************************************************************************
	
	// Load super-region information
	use "FILEPATH/locations_22.dta", clear
	keep location_id super_region_name
	tempfile sr
	save `sr', replace

	// Load MDR-HIVTB envelope
	use `mdrtb', clear
	duplicates drop location_id year_id age_group_id sex_id, force

	// Merge super-region and XDR PAF
	merge m:1 location_id using `sr', keep(3)nogen
	merge m:1 super_region_name year_id using "FILEPATH/par_xdr_draws.dta", keep(3)nogen

	// Compute XDR deaths
	forvalues i = 0/999 {
		qui gen draw_`i'=mdrtb_`i'*PAR_based_on_mean_rr_`i'
	}

	// Drop
	drop PAR_based_on_mean_rr_* mdrtb_*

	// No XDR cases before 1993 so set to 0
	preserve 
	keep if year<1993
	forvalues i=0/999 {
		replace draw_`i'=0 
	}
	tempfile zero
	save `zero', replace
	restore

	// Append zeros to XDR-HIVTB case
	drop if year<1993 
	append using `zero'

	// Clean
	replace cause_id=950
	keep cause_id location_id year_id age_group_id sex_id draw_*
	tempfile xdr
	save `xdr', replace

	// Save as csvs
	levelsof(location_id), local(ids) clean
	foreach location_id of local ids {	
		qui outsheet if location_id==`location_id' using "FILEPATH/`location_id'.csv", comma replace
	}

**********************************************************************************************************************
** COMPUTE MDR-HIVTB
**********************************************************************************************************************

	// Prep XDR-HIVTB deahts
	use `xdr', clear
	forvalues i = 0/999 {
		rename draw_`i' xdr_`i'
	}

	// Merge XDR-HIVTB deaths to MDR-HIVTB envelope
	merge 1:1 location_id year_id age_group_id sex using `mdrtb', keep(3) nogen 

	// Compute MDR-HIVTB deaths by subtracting XDR-HIVTB
	forvalues i=0/999 {
		qui gen draw_`i'=mdrtb_`i'-xdr_`i'
		replace draw_`i'=0 if draw_`i'==.
	}
	
	// Clean
	drop mdr* xdr*
	replace cause_id=949	

	// Save as csvs
	levelsof(location_id), local(ids) clean
	foreach location_id of local ids {	
		qui outsheet if location_id==`location_id' using "FILEPATH/`location_id'.csv", comma replace
	}

**********************************************************************************************************************
** UPLOAD RESULTS FOR DS-HIVTB, MDR-HIVTB, XDR-HIVTB
**********************************************************************************************************************

	// save results for XDR-HIVTB
	run "FILEPATH/save_results_cod.ado"
	save_results_cod, cause_id(950) description(`decomp_step': hivtb_xdr custom `custom_version'; with latest HIV model and updated MDR props, MR-BRT RR; updated HIV RR) input_file_pattern({location_id}.csv) input_dir(FILEPATH) model_version_type_id(6) decomp_step(`decomp_step') clear
	
	// save results for MDR-HIVTB
	run "FILEPATH/save_results_cod.ado"
	save_results_cod, cause_id(949) description(`decomp_step': hivtb_mdr custom `custom_version'; with latest HIV model and updated MDR props, MR-BRT RR; updated HIV RR) input_file_pattern({location_id}.csv) input_dir(FILEPATH) model_version_type_id(6) decomp_step(`decomp_step') clear

	// save results for DS-HIVTB	
	run "FILEPATH/save_results_cod.ado"
	save_results_cod, cause_id(948) description(`decomp_step': hivtb_drug_sensitive custom `custom_version'; with latest HIV model and updated MDR props, MR-BRT RR; updated HIV RR) input_file_pattern({location_id}.csv) input_dir(FILEPATH) model_version_type_id(6) decomp_step(`decomp_step') clear

	