// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
// Purpose:		Splits TB no-HIV parent into TB child causes while incorporating retreated cases into incidence estimates
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
** STEP 1: COMPUTE MDR-TB no-HIV ALL BY APPLYING PROPORTIONS FROM ST-GPR MODELS
**********************************************************************************************************************

	// Get population
	clear all
	adopath + "FILEPATH"
	get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") decomp_step(`decomp_step') clear
	rename population mean_pop

	// Save tempfile for populations 
	tempfile pop_all
	save `pop_all', replace

	// Pull TB no-HIV draws
	use "FILEPATH/TBnoHIV_`model_version_id'.dta", clear 

	// Merge populations
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Convert rates to cases
	forvalues i = 0/999 {
		replace draw_`i'=draw_`i'*mean_pop
	}

	// Save tempfile
	tempfile tb_noHIV
	save `tb_noHIV', replace

	// Merge on MDR proportion
	merge m:1 location_id year_id age_group_id sex_id using "FILEPATH/tb_nohiv_mdr_prop_draws.dta", keep(3)nogen

	// Apply mdr proportions
	forvalues i = 0/999 {
		 replace draw_`i'=draw_`i'*tb_nohiv_mdr_prop_`i'
	}
	
	// Clean
	drop tb_nohiv_mdr_prop_*

	// Rename draws
	forvalues i = 0/999 {
		 rename draw_`i' mdrtb_`i'
	}
	
	// Save tempfile
	tempfile mdrtb
	save `mdrtb', replace

**********************************************************************************************************************
** STEP 2: SUBTRACT MDR-TB ALL FROM ALL TB NO-HIV TO COMPUTE DRUG-SENSITIVE TB
**********************************************************************************************************************

	// Merge TB no-HIV and MDR-TB
	use `tb_noHIV', clear
	merge 1:1 measure_id location_id year_id age_group_id sex_id using `mdrtb', keep(3) nogen 

	// Loop through draws and subtract MDR-TB from TB no-HIV
	forvalues i=0/999 {
		replace draw_`i'=draw_`i'-mdrtb_`i'
	}

	// Convert to rate space
	forvalues i=0/999 {
		replace draw_`i'=draw_`i'/mean_pop
		replace draw_`i'=0 if draw_`i'<0
	}
	
	// Clean data
	keep measure_id location_id year_id age_group_id sex_id draw_*
	gen modelable_entity_id=10829

	// Split into prevalence only data
	preserve
	keep if measure_id==5
	tempfile prev
	save `prev', replace

	// Split int incidence only data
	restore
	keep if measure_id==6
	tempfile inc
	save `inc', replace

	// Prep prevalence for drug-sensitive TB
	use `prev',clear
	levelsof(location_id), local(ids) clean

	// Save csvs
	foreach location_id of local ids {	
		qui outsheet if location_id==`location_id' using "FILEPATH/`location_id'_5.csv", comma replace
	}
			
	// Prep incidence for drug-sensitive TB
	use `inc',clear
	levelsof(location_id), local(ids) clean

	// Save csvs
	foreach location_id of local ids {
		qui outsheet if location_id==`location_id' using "FILEPATH/`location_id'_6.csv", comma replace
	}
	
**********************************************************************************************************************
** STEP 3: COMPUTE XDR-HIVTB BY USING EXTRAPOLATED PROPORTIONS
**********************************************************************************************************************

	// Pull Super-region info
	use "FILEPATH/locations_22.dta", clear
	keep location_id super_region_name

	// Save tempfile
	tempfile sr
	save `sr', replace

	// Pull in MDR-TB all
	use `mdrtb', clear
	merge m:1 location_id using `sr', keep(3)nogen

	// Merge on XDR proportions
	merge m:1 super_region_name year_id using "FILEPATH/xdr_prop_decomp1.dta", keep(3)nogen

	// Apply proportions to derive XDR-TB
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
	gen modelable_entity_id=10831
	tempfile xdr
	save `xdr', replace

**********************************************************************************************************************
** STEP 4: COMPUTE MDR-TB WITHOUT XDR-TB
**********************************************************************************************************************
	
	// Rename XDR draws
	forvalues i = 0/999 {
		rename draw_`i' xdr_`i'
	}

	// Merge on MDR-TB no-HIV all
	merge 1:1 measure_id location_id year_id age_group_id sex using `mdrtb', keep(3) nogen 

	// Subtract XDR-TB from MDR-TB all
	forvalues i=0/999 {
		gen draw_`i'=mdrtb_`i'-xdr_`i'
		replace draw_`i'=0 if draw_`i'<0
	}

	// Clean dataset
	keep measure_id location_id year_id age_group_id sex_id draw_*
	gen modelable_entity_id=10830

	// Convert MDR-TB into rate space
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Do conversion
	forvalues i=0/999 {
		replace draw_`i'=draw_`i'/mean_pop
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
	
**********************************************************************************************************************
** STEP 5: PREP MDR-TB NO-HIV FOR UPLOAD BY INCORPORATING RETREATED CASES
**********************************************************************************************************************

	// Prep prevalence for MDR-TB
	use `mdr_prev',clear
	levelsof(location_id), local(ids) clean

	// Save csvs
	foreach location_id of local ids {
		qui outsheet if location_id==`location_id' using "FILEPATH/`location_id'_5.csv", comma replace
	}
			
	// Pull retreated MDR-TB cases
	use "FILEPATH/mdr_all_`ret_model_version_id'.dta", clear
	replace modelable_entity_id=10830
	levelsof(location_id), local(ids) clean

	// Prep incidence for MDR-TB
	foreach location_id of local ids {
		qui outsheet if location_id==`location_id' using "FILEPATH/`location_id'_6.csv", comma replace
	}

	// Pull retreated MDR-TB cases
	use "FILEPATH/mdr_all_`ret_model_version_id'.dta", clear

	// Prep to add in retreated cases
	forvalues i=0/999 {
		rename draw_`i' ret_draw_`i'
	}

	// Merge on MDR-TB incidence
	merge 1:1 location_id year_id age_group_id sex_id using `mdr_inc', keep(3) nogen

	// Incorporate retreated cases
	forvalues i=0/999 {
		gen draw_`i'= ret_draw_`i'-no_ret_draw_`i'
	}	

	// Save MDR-TB
	save "FILEPATH/missing_mdr_inc_`model_version_id'.dta", replace

**********************************************************************************************************************
** STEP 6: PREP XDR-TB NO-HIV FOR UPLOAD BY INCORPORATING RETREATED CASES
**********************************************************************************************************************

	// Pull XDR-TB 
	use `xdr', clear
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Convert XDR-TB into rate space
	forvalues i=0/999 {
		replace draw_`i'=draw_`i'/mean_pop
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

	// Prep prevalence for XDR-TB
	use `xdr_prev',clear
	levelsof(location_id), local(ids) clean

	// Save csvs
	foreach location_id of local ids {
		qui outsheet if location_id==`location_id' using "FILEPATH/`location_id'_5.csv", comma replace
	}
			
	// Pull retreated XDR-TB cases
	use "FILEPATH/xdr_all_`ret_model_version_id'.dta", clear
	replace modelable_entity_id=10831
	levelsof(location_id), local(ids) clean

	// Save csvs
	foreach location_id of local ids {
		qui outsheet if location_id==`location_id' using "FILEPATH/`location_id'_6.csv", comma replace
	}
			
	// Pull retreated XDR-TB cases
	use "FILEPATH/xdr_all_`ret_model_version_id'.dta", clear

	// Prep to add in retreated cases
	forvalues i=0/999 {
		rename draw_`i' ret_draw_`i'
	}

	// Merge on XDR-TB incidence
	merge 1:1 location_id year_id age_group_id sex_id using `xdr_inc', keep(3) nogen

	// Incorporate retreated cases
	forvalues i=0/999 {
		gen draw_`i'= ret_draw_`i'-no_ret_draw_`i'
	}	

	// Save XDR-TB
	save "FILEPATH/missing_xdr_inc_`model_version_id'.dta", replace

**********************************************************************************************************************
** STEP 7: UPLOAD TO DATABASE
**********************************************************************************************************************

	// Upload drug-sensitive TB
	adopath + "FILEPATH"
	save_results_epi, input_dir("FILEPATH/`model_version_id'") input_file_pattern({location_id}_{measure_id}.csv) modelable_entity_id(10829) mark_best("True") description("`decomp_step' drug susceptible, `model_version_id'") measure_id(5 6) db_env("prod") decomp_step(`decomp_step') clear 

	// Upload XDR-TB
	adopath + "FILEPATH"
	save_results_epi, input_dir("FILEPATH/`model_version_id'") input_file_pattern({location_id}_{measure_id}.csv) modelable_entity_id(10830) mark_best("True") description("`decomp_step' mdr, `model_version_id'") measure_id(5 6) db_env("prod") decomp_step(`decomp_step') clear 

	// Upload MDR-TB
	adopath + "FILEPATH"
	save_results_epi, input_dir("FILEPATH/`model_version_id'") input_file_pattern({location_id}_{measure_id}.csv) modelable_entity_id(10831) mark_best("True") description("`decomp_step' xdr, `model_version_id'") measure_id(5 6) db_env("prod") decomp_step(`decomp_step') clear 
