// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
// Purpose:		Compute MDR-TB no-HIV (without XDR-TB) and XDR-TB no-HIV
// Author:		USERNAME
// Edited:      USERNAME
// Description:	computes XDR-TB no-HIV by applying XDR PAF to MDR-TB no-HIV envelope
//				subtracts XDR-TB no-HIV from MDR-TB no-HIV envelope to derive MDR-TB no-HIV (without XDR-TB)
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
	local acause tb
	local custom_version 637511_636083
	local decomp_step step4

	// Make folders on cluster
	capture mkdir "FILEPATH/xdr/`custom_version'"
	capture mkdir "FILEPATH/mdr/`custom_version'"
	
**********************************************************************************************************************
** LOAD IN MDR-TB NO-HIV ENVELOPE AND APPLY XDR PAF
**********************************************************************************************************************

	// Load in super region information
	use "FILEPATH/locations_22.dta", clear
	keep location_id super_region_name
	tempfile sr
	save `sr', replace

	// Load in MDR-TB no-HIV envelope
	insheet using FILEPATH/mdrtb_cyas_`custom_version'.csv, comma names clear
	duplicates drop location_id year_id age_group_id sex_id, force
	tempfile mdr_cyas
	save `mdr_cyas', replace

	// Merge super-region information
	merge m:1 location_id using `sr', keep(3)nogen

	// Merge in XDR PAF
	merge m:1 super_region_name year_id using "FILEPATH/par_xdr_draws.dta", keep(3)nogen
	drop super_region_name

	// Apply PAF to derive XDRTB no-HIV deaths
	forvalues i = 0/999 {
		replace draw_`i'=draw_`i'*PAR_based_on_mean_rr_`i'
	}

	// Clean
	drop PAR_based_on_mean_rr_*

	// Before 1992, no XDR-TB so zero those years out
	preserve 
	keep if year<1993
	forvalues i=0/999 {
		replace draw_`i'=0 
	}
	tempfile zero
	save `zero', replace
	restore

	// Append zeros
	drop if year<1993
	append using `zero'

	// Clean
	replace cause_id=947
	keep location_id year_id age_group_id sex_id cause_id draw_*
	tempfile xdr
	save `xdr', replace

	// Save csvs
	levelsof(location_id), local(ids) clean
	foreach location_id of local ids {	
		qui outsheet if location_id==`location_id' using "FILEPATH/`location_id'.csv", comma replace
	}

**********************************************************************************************************************
** SUBTRACT XDR-TB NO-HIV DEATHS FROM MDR-TB ENVELOPE
**********************************************************************************************************************

	// Prep XDR-TB deaths for merge
	use `xdr', clear
	forvalues i = 0/999 {
		rename draw_`i' xdr_`i'
	}

	// Merge XDR-TB deaths
	merge 1:1 location_id year_id age_group_id sex_id using `mdr_cyas', keep(3) nogen 

	// Loop through draws and subtract xdr from mdr
	forvalues i=0/999 {
		replace draw_`i'=draw_`i'-xdr_`i'
	}	

	// Clean
	replace cause_id=946
	keep location_id year_id age_group_id sex_id cause_id draw_*
	levelsof(location_id), local(ids) clean

	// Save csvs
	foreach location_id of local ids {	
		qui outsheet if location_id==`location_id' using "FILEPATH/`location_id'.csv", comma replace
	}

**********************************************************************************************************************
** UPLOAD RESULTS FOR MDR-TB NO-HIV AND XDR-TB NO-HIV
**********************************************************************************************************************

	// save results for XDR-TB no-hiv
	run "FILEPATH"
	save_results_cod, cause_id(947) description(`decomp_step': xdr custom `custom_version'; updated MDR props, MR-BRT RR) input_file_pattern({location_id}.csv) input_dir(FILEPATH) decomp_step(`decomp_step') mark_best(True) clear

	// save results for MDR-TB no-hiv
	run "FILEPATH.ado"
	save_results_cod, cause_id(946) description(`decomp_step': mdr custom `custom_version'; updated MDR props, MR-BRT RR) input_file_pattern({location_id}.csv) input_dir(FILEPATH) decomp_step(`decomp_step') mark_best(True) clear

