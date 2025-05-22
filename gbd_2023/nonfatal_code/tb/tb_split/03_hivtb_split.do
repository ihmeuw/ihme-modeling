// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
// Purpose:		Splits HIVTB parent into HIVTB child causes
// Author:		USERNAME
// Edited:      USERNAME
// Description:	Apply MDR-TB proportions to HIVTB 
//				Subtract MDR-HIVTB to compute drug-susceptible HIVTB
//				Compute XDR-HIVTB and MDR-HIVTB without XDR-HIVTB
// Variables:	acause, model_version_id, ret_model_version_id, decomp_step
// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
         
**********************************************************************************************************************
** ESTABLISH SETTINGS FOR SCRIPT
**********************************************************************************************************************		 
		 
// Load settings

	// Clear memory and establish settings
	clear all
	set more off
	set mem 5G
	set maxvar 32000
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
	local model_version_id dismod_478688
	local ret_model_version_id dismod_478688_ret
	local decomp_step step4

**********************************************************************************************************************
** STEP 1: COMPUTE MDR-HIVTB ALL BY APPLYING PROPORTIONS FROM ST-GPR MODELS
**********************************************************************************************************************

	// Get population
	clear all
	adopath + "FILEPATH"
	get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") decomp_step(`decomp_step') clear
	rename population mean_pop

	// Save tempfile for populations 
	tempfile pop_all
	save `pop_all', replace

	// Pull HIV-TB draws
	use FILEPATH/HIVTB_prev_cyas_`model_version_id'_capped.dta, clear
	append using FILEPATH/HIVTB_inc_cyas_`model_version_id'_capped.dta

	// Merge populations
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Convert rates to cases
	forvalues i = 0/999 {
		replace draw_`i'=draw_`i'*mean_pop
	}

	// Save tempfile
	tempfile hivtb
	save `hivtb', replace

	// Merge on MDR proportions
	merge m:1 location_id year_id age_group_id sex_id using "FILEPATH/hiv_mdr_prop_draws.dta", keep(3)nogen

	// Apply mdr proportions
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
** STEP 2: SUBTRACT MDR-HIVTB ALL FROM ALL HIVTB TO COMPUTE DRUG-SENSITIVE HIVTB
**********************************************************************************************************************

	// Merge HIVTB and MDR-HIVTB
	use `hivtb', clear
	merge 1:1 measure_id location_id year_id age_group_id sex_id using `mdrtb', keep(3) nogen 

	// Loop through draws and subtract MDR-HIVTB from HIVTB 
	forvalues i=0/999 {
		replace draw_`i'=draw_`i'-mdrtb_`i'
		replace draw_`i'=0 if draw_`i'<0
	}

	// Convert to rate space
	forvalues i=0/999 {
		replace draw_`i'=draw_`i'/mean_pop
		replace draw_`i'=0 if draw_`i'<0
	}
	
	// Clean data	
	keep measure_id location_id year_id age_group_id sex_id draw_*
	gen modelable_entity_id=10832

	// Split into prevalence only data
	preserve
	keep if measure_id==5
	tempfile ds_prev
	save `ds_prev', replace

	// Split into incidence only data
	restore
	keep if measure_id==6
	tempfile ds_inc
	save `ds_inc', replace

**********************************************************************************************************************
** STEP 3: COMPUTE XDR-HIVTB BY USING EXTRAPOLATED PROPORTIONS
**********************************************************************************************************************

	// Pull Super-region info
	use "FILEPATH/locations_22.dta", clear
	keep location_id super_region_name

	// Save tempfile
	tempfile sr
	save `sr', replace

	// Pull in MDR-HIVTB all
	use `mdrtb', clear
	merge m:1 location_id using `sr', keep(3)nogen

	// Merge on XDR proportions
	merge m:1 super_region_name year_id using "FILEPATH/xdr_prop_decomp1.dta", keep(3)nogen

	// Apply proportions to derive XDR-HIVTB
	forvalues i=0/999 {
		gen draw_`i'=mdrtb_`i'*xdr_prop
		replace draw_`i'=0 if draw_`i'<0
	}
				
	// Clean
	keep measure_id location_id year_id age_group_id sex_id draw_*

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

	// Create tempfile
	gen modelable_entity_id=10834
	tempfile xdr
	save `xdr', replace

**********************************************************************************************************************
** STEP 4: COMPUTE MDR-HIVTB WITHOUT XDR-HIVTB
**********************************************************************************************************************

	// Rename XDR draws
	forvalues i = 0/999 {
		rename draw_`i' xdr_`i'
	}

	// Merge on MDR-HIVTB all
	merge 1:1 measure_id location_id year_id age_group_id sex using `mdrtb', keep(3) nogen 

	// Subtract XDR-HIVTB from MDR-HIVTB all
	forvalues i=0/999 {
		gen draw_`i'=mdrtb_`i'-xdr_`i'
		replace draw_`i'=0 if draw_`i'<0
	}

	// Clean dataset
	keep measure_id location_id year_id age_group_id sex_id draw_*
	gen modelable_entity_id=10833

	// Convert MDR-HIVTB into rate space
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Do conversion
	forvalues i=0/999 {
		replace draw_`i'=draw_`i'/mean_pop
		replace draw_`i'=0 if draw_`i'<0
	}

	// Create prevalence only dataset
	drop mean_pop
	preserve
	keep if measure_id==5
	tempfile mdr_prev
	save `mdr_prev', replace

	// Create incidence only dataset
	restore
	keep if measure_id==6

	// Prep to incorporate retreated cases
	forvalues i=0/999 {
		rename draw_`i' no_ret_draw_`i'
	}	

	// Save tempfile
	tempfile mdr_inc
	save `mdr_inc', replace
			
	// Pull retreated HIVTB cases
	use "FILEPATH/mdr_hivtb_inc_`ret_model_version_id'.dta", clear

	// Prep to add in retreated cases
	forvalues i=0/999 {
		rename draw_`i' ret_draw_`i'
	}

	// Merge on MDR-HIVTB incidence
	merge 1:1 location_id year_id age_group_id sex_id using `mdr_inc', keep(3) nogen

	// Compute missing cases
	forvalues i=0/999 {
		gen draw_`i'= ret_draw_`i'-no_ret_draw_`i'
	}	

	// Save missing cases
	save "FILEPATH/missing_mdr_hivtb_inc_`model_version_id'.dta", replace

**********************************************************************************************************************
** STEP 5: INCORPORATE RETREATED CASES FOR XDR-HIVTB
**********************************************************************************************************************

	// Pull XDR-HIVTB
	use `xdr', clear
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Convert XDR-HIVTB into rate space
	forvalues i=0/999 {
		replace draw_`i'=draw_`i'/mean_pop
		replace draw_`i'=0 if draw_`i'<0
	}

	// Create prevalence only dataset
	drop mean_pop
	preserve
	keep if measure_id==5
	tempfile xdr_prev
	save `xdr_prev', replace

	// Create incidence only dataset
	restore
	keep if measure_id==6

	// Prep to incorporate retreated cases
	forvalues i=0/999 {
		rename draw_`i' no_ret_draw_`i'
	}	

	// Save tempfile
	tempfile xdr_inc
	save `xdr_inc', replace

	// Pull retreated HIVTB cases
	use "FILEPATH/xdr_hivtb_inc_`ret_model_version_id'.dta", clear

	// Prep to add in retreated cases
	forvalues i=0/999 {
		rename draw_`i' ret_draw_`i'
	}
				
	// Merge on XDR-HIVTB incidence
	merge 1:1 location_id year_id age_group_id sex_id using `xdr_inc', keep(3) nogen

	// Compute missing cases
	forvalues i=0/999 {
		gen draw_`i'= ret_draw_`i'-no_ret_draw_`i'
	}
	
	// Save missing XDR-HIVTB cases
	save "FILEPATH/missing_xdr_hivtb_inc_`model_version_id'.dta", replace

**********************************************************************************************************************
** STEP 6: PREP DRAWS FOR UPLOADS
**********************************************************************************************************************

	// Prep prevalence for drug-sensitive HIVTB
	use `ds_prev',clear
	levelsof(location_id), local(ids) clean

	// Save csvs
	foreach location_id of local ids {
		qui outsheet if location_id==`location_id' using "FILEPATH/5_`location_id'.csv", comma replace
	}

	// Prep incidence for drug-sensitive HIVTB
	use `ds_inc',clear
	levelsof(location_id), local(ids) clean

	// Save csvs
	foreach location_id of local ids {	
		qui outsheet if location_id==`location_id' using "FILEPATH/6_`location_id'.csv", comma replace
	}
			
	// Prep prevalence for MDR-HIVTB
	use `mdr_prev',clear
	levelsof(location_id), local(ids) clean

	// Save csvs
	foreach location_id of local ids {
		qui outsheet if location_id==`location_id' using "FILEPATH/5_`location_id'.csv", comma replace
	}
			
	// Prep incidence for MDR-HIVTB
	use "FILEPATH/mdr_hivtb_inc_`ret_model_version_id'.dta", clear
	replace modelable_entity_id=10833
	levelsof(location_id), local(ids) clean

	// Save csvs
	foreach location_id of local ids {	
		qui outsheet if location_id==`location_id' using "FILEPATH/6_`location_id'.csv", comma replace
	}
					
	// Prep prevalence for XDR-HIVTB
	use `xdr_prev',clear
	levelsof(location_id), local(ids) clean

	// Save csvs
	foreach location_id of local ids {
		qui outsheet if location_id==`location_id' using "FILEPATH/5_`location_id'.csv", comma replace
	}
			
	// Prep incidence for XDR-HIVTB
	use "FILEPATH/xdr_hivtb_inc_`ret_model_version_id'.dta", clear
	replace modelable_entity_id=10834
	levelsof(location_id), local(ids) clean

	// Save csvs
	foreach location_id of local ids {	
		qui outsheet if location_id==`location_id' using "FILEPATH/6_`location_id'.csv", comma replace
	}

**********************************************************************************************************************
** STEP 7: UPLOAD TO DATABASE
**********************************************************************************************************************

	// Upload drug-sensitive HIVTB
	adopath + "FILEPATH"
	save_results_epi, input_dir("FILEPATH/`model_version_id'") input_file_pattern({measure_id}_{location_id}.csv) modelable_entity_id(10832) description(`decomp_step': drug_sensitive hivtb, `model_version_id') measure_id(5 6) db_env("prod") decomp_step(`decomp_step') clear 

	// Upload XDR-HIVTB
	adopath + "FILEPATH"
	save_results_epi, input_dir("FILEPATH/`model_version_id'") input_file_pattern({measure_id}_{location_id}.csv) modelable_entity_id(10834) description(`decomp_step': xdr hivtb, `model_version_id') measure_id(5 6) db_env("prod") decomp_step(`decomp_step') clear 

	// Upload MDR-HIVTB
	adopath + "FILEPATH"
	save_results_epi, input_dir("FILEPATH/`model_version_id'") input_file_pattern({measure_id}_{location_id}.csv) modelable_entity_id(10833) description(`decomp_step': mdr hivtb, `model_version_id') measure_id(5 6) db_env("prod") decomp_step(`decomp_step') clear 
