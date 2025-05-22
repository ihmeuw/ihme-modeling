// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
// Purpose:		This code disaggregates the TB no-HIV model CODEm into drug sensitive TB using proportions from ST-GPR model
// Author:		USERNAME
// Edited:      USERNAME
// Description:	computes MDR-TB no-HIV envelope by pulling CODEm draws and ST-GPR draws
//				computes drug-sensitive TB no-HIV deaths by subtracting CODEm evelope and MDR-TB no-HIV envelope
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
	capture mkdir "FILEPATH/`custom_version'"
	capture mkdir "FILEPATH/tb_parent"
	capture mkdir "FILEPATH/`custom_version'"
	capture mkdir "FILEPATH/`custom_version'"

	// Load central functions		
	adopath + "FILEPATH"
	
**********************************************************************************************************************
** LOAD IN DRAWS AND COMPUTE MDR-TB NO-HIV ENVELOPE
**********************************************************************************************************************

	// Load CODEm draws
	use "FILEPATH/tb_codem_draws_`custom_version'.dta"
	tempfile tb
	save `tb', replace
	
	// Merge ST-GPR PAF draws
	merge 1:1 location_id year_id age_group_id sex_id using "FILEPATH/par_tb_mdr_draws.dta", keep(3)nogen

	// Compute MDR-TB no-HIV deaths
	forvalues i = 0/999 {
		replace draw_`i'=draw_`i'*PAR_based_on_mean_rr_`i'
	}
	
	// Save MDR-TB no-HIV draws
	drop PAR_based_on_mean_rr_*
	outsheet using FILEPATH/mdrtb_cyas_`custom_version'.csv, comma names replace

	// Rename MDR-TB no-HIV draws
	forvalues i = 0/999 {
		rename draw_`i' mdrtb_`i'
	}
	
	// Save as tempfile for later
	tempfile mdrtb
	save `mdrtb', replace

**********************************************************************************************************************
** SUBTRACT MDR-TB DEATHS FROM TB NO-HIV ENVELOPE TO COMPUTE DRUG-SUSCEPTIBLE TB NO-HIV
**********************************************************************************************************************

	// Merge TB no-HIV envelope and MDR-TB no-HIV deaths
	use `tb', clear
	merge 1:1 location_id year_id age_group_id sex_id using `mdrtb', keep(3) nogen 

	// Compute drug-susceptible TB by looping through draws and subtract TB from MDR-TB 
	forvalues i=0/999 {
		replace draw_`i'=draw_`i'-mdrtb_`i'
		replace draw_`i'=0 if draw_`i'<0
	}
	
	// Clean	
	replace cause_id=934		
	keep location_id year_id age_group_id sex_id cause_id draw_*

	// Create location specific csvs for upload
	levelsof(location_id), local(ids) clean
	
	// Save csvs
	foreach location_id of local ids {	
		qui outsheet if location_id==`location_id' using "FILEPATH/`location_id'.csv", comma replace
	}

**********************************************************************************************************************
** UPLOAD RESULTS FOR DRUG-SUSCEPTIBLE TB NO-HIV
**********************************************************************************************************************

	// save results for drug-susceptible TB
	run "FILEPATH/save_results_cod.ado"
	save_results_cod, cause_id(934) description(`my_decomp_step': tb_drug_susp custom `custom_version') input_file_pattern({location_id}.csv) input_dir(FILEPATH/`custom_version') decomp_step(`decomp_step') mark_best(True) clear

