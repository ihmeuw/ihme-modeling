// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
// Purpose:		Computes population level incidence and prevalence, and disaggregates all-form TB into HIV status
// Author:		USERNAME
// Edited:      USERNAME
// Description:	Multiply DisMod TB estimates by LTBI prevalence to derive population level morbidity
//				Uses predicted HIV-TB proportions (from CoD mixed effects regression) to split all-form TB into HIV-TB
//				Uses HIV prevalence age pattern to age split HIV-TB cases
//				Subtracts HIV-TB from all-form TB envelope to derive TB no-HIV
// 				saves HIV-TB and TB no-HIV envelopes to EPI database
// Variables:	acause, dismod_mvid, model_version_id, ltbi, hiv, decomp_step
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
	local dismod_mvid 494408
	local model_version_id dismod_494408 // me_id=9422 (dismod)
	local ltbi 493040 					 // me_id=24730 (dismod) Use LTBI model that was used to divide input data
	local hiv 483185  					 // me_id =9368 (custom type)
	local xwalk_id 17819                 // crosswalk version id used in 9422
	local decomp_step step2		

**********************************************************************************************************************
** STEP 1A: GATHER INPUTS (TB DISMOD DRAWS AND LTBI DRAWS) FOR COMPUTATIONS
**********************************************************************************************************************

	// Get age groups
	adopath + "FILEPATH"
	get_demographics, gbd_team(epi) clear
	local ages `=subinstr("`r(age_group_id)'", " ", ",", .)'

	// Pull TB draws from DisMod
	adopath + "FILEPATH"
	get_draws, gbd_id_type(modelable_entity_id) gbd_id(9422) measure_id(5 6) num_workers(15) source(epi) version_id(`dismod_mvid') decomp_step(`decomp_step') clear

	// Drop aggregate locations
	drop if inlist(location_id, 1, 4, 5, 9, 21, 31, 32, 42, 56, 64, 65, 70, 73, 96, 100, 103, 104, 120, 124, 134, 137, 138, 158, 159, 166, 167, 174, 192, 199)
	drop if inlist(location_id, 6, 16, 51, 62, 67, 72, 86, 90, 93, 95, 102, 130, 135, 142, 163, 165, 179, 180, 196, 214)

	// Save draws
	save /share/epi/tb/temp/tb_epi_draws_`model_version_id'.dta, replace

	// Pull LTBI draws from DisMod
	adopath + "FILEPATH"
	get_draws, gbd_id_type(modelable_entity_id) gbd_id(24730) version_id(`ltbi') source(epi) decomp_step(`decomp_step') clear

	// drop aggregate locations
	drop if inlist(location_id, 1, 4, 5, 9, 21, 31, 32, 42, 56, 64, 65, 70, 73, 96, 100, 103, 104, 120, 124, 134, 137, 138, 158, 159, 166, 167, 174, 192, 199)
	drop if inlist(location_id, 6, 16, 51, 62, 67, 72, 86, 90, 93, 95, 102, 130, 135, 142, 163, 165, 179, 180, 196, 214)
	drop measure*

	// Rename LTBI draws
	forvalues i = 0/999 {
		rename draw_`i' ltbi_`i'
	}

	// Save draws
	save FILEPATH/ltbi_draws_`ltbi'.dta, replace		

**********************************************************************************************************************
** STEP 1B: DERIVE POPULATION LEVEL ESTIMATES
**********************************************************************************************************************

	// Load in TB draws
	use "FILEPATH/tb_epi_draws_`model_version_id'.dta", clear

	// Merge on LTBI draws
	merge m:1 location_id year_id age_group_id sex_id using "FILEPATH/ltbi_draws_`ltbi'.dta", keep(3)nogen

	// Multiply to compute population level estimates
	forvalues i=0/999 {
		di in red "draw `i'"
		replace draw_`i'=draw_`i'*ltbi_`i'
	}

	// Clean and save
	drop ltbi_*			
	tempfile tb_all
	save `tb_all', replace
	save "FILEPATH/tb_`model_version_id'.dta", replace

	// Clean
	keep if inlist(age_group_id, `ages')
	replace modelable_entity_id=9806
	duplicates drop location_id year_id age_group_id sex_id measure_id, force
	drop model_version_id

	// Sort data
	order modelable_entity_id measure_id location_id year_id age_group_id sex_id
	sort measure_id location_id year_id age_group_id sex_id

	// Create tempfile for prevalence
	preserve
		keep if measure_id==5
		tempfile prev
		save `prev', replace
	restore

	// Create tempfile for incidence
	keep if measure_id==6
	tempfile inc
	save `inc', replace

	// Create csvs for prevalence
	use `prev',clear
	levelsof(location_id), local(ids) clean

	foreach location_id of local ids {
		qui outsheet if location_id==`location_id' using "FILEPATH/`location_id'_5.csv", comma replace
	}	

	// Create csvs for incidence		
	use `inc',clear
	levelsof(location_id), local(ids) clean

	foreach location_id of local ids {
		qui outsheet if location_id==`location_id' using "FILEPATH/`location_id'_6.csv", comma replace
	}
		
	// Upload TB all-forms to database
	run "FILEPATH/save_results_epi.ado"
	save_results_epi, input_dir("FILEPATH/`model_version_id'") input_file_pattern({location_id}_{measure_id}.csv) modelable_entity_id(9806) mark_best("True") description("`decomp_step': `model_version_id'; ltbi model used `ltbi'") measure_id(5 6) db_env("prod") decomp_step(`decomp_step') clear 

**********************************************************************************************************************
** STEP 2A: GATHER INPUTS FOR HIV-TB SPLIT
**********************************************************************************************************************

	// Pull TB all-forms
	adopath + "FILEPATH"
	get_draws, gbd_id_type(modelable_entity_id) gbd_id(9806) measure_id(5 6) source(epi) decomp_step(`decomp_step') clear

	// Clean and save draws
	keep measure_id location_id year_id age_group_id sex_id draw_*
	save "FILEPATH/tb_`model_version_id'.dta", replace 

	// Load in draws
	use "FILEPATH/tb_`model_version_id'.dta", clear
	keep if inlist(age_group_id, `ages')

	// Create a prevalence only data frame
	preserve
		keep if measure_id==5
		duplicates drop location_id year_id age_group_id sex_id measure_id, force
		tempfile prev
		save `prev', replace
	restore

	// Create a incidence only data frame
	keep if measure_id==6
	duplicates drop location_id year_id age_group_id sex_id measure_id, force
	tempfile inc
	save `inc', replace

	// Pull HIV prevalence age pattern
	clear all
	adopath + "FILEPATH"
	get_model_results, gbd_team("epi") gbd_id(9368) model_version_id(`hiv') decomp_step(`decomp_step') clear

	// Clean and save
	duplicates drop 
	save "FILEPATH/hiv_prev_age_pattern.dta", replace

	// Load age pattern and clean
	use "FILEPATH/hiv_prev_age_pattern.dta", clear
	drop model_version_id
	keep if measure_id==5
	keep if inlist(age_group_id, `ages')
	rename mean rate
	drop lower upper

	// Save tempfile
	tempfile age_pattern
	save `age_pattern', replace

	// Pull population  
	clear all
	adopath + "FILEPATH"
	get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") decomp_step(`decomp_step') clear

	// Clean and save tempfile
	rename population mean_pop
	tempfile pop_all
	save `pop_all', replace

	// Subset to data needed
	use `pop_all', clear
	drop if year_id<1980
	drop if location_id==1
	drop if sex_id==3
	tempfile tmp_pop
	save `tmp_pop', replace

*********************************************************************************************************************
** STEP 2B: COMPUTE HIV-TB PREVALENCE
**********************************************************************************************************************

	// Run ado file for fast collapse
	adopath + "FILEPATH"

	// Load prevalence estimates 
	use `prev', clear
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Convert to case space
	forvalues i=0/999 {
		di in red "draw `i'"
		replace draw_`i'=draw_`i'*mean_pop
	}

	// Tempfile for cases
	tempfile prev_cases
	save `prev_cases', replace
	keep if inlist(age_group_id, `ages')

	// Collapse draws by location_id and year_id
	fastcollapse draw_*, type(sum) by(location_id year_id) 

	// Merge on proportions
	merge 1:1 location_id year_id using "FILEPATH/Prop_tbhiv_mean_ui.dta", keepusing(mean_prop) keep(3)nogen 
			
	// Apply HIV-TB proportions to TB no-HIV envelope
	forvalues i=0/999 {
		di in red "draw `i'"
		gen tbhiv_d`i'=mean_prop*draw_`i'
		drop draw_`i' 
	}
			
	// Prep for age split
	merge 1:m location_id year_id using `tmp_pop', keep(1 3) nogen
	merge m:1 location_id year_id age_group_id sex_id using `age_pattern', keep(3)nogen

	rename mean_pop sub_pop
	gen rate_sub_pop=rate*sub_pop

	// Compute denominator 
	preserve
	collapse (sum) rate_sub_pop, by(location_id year_id) fast
	rename rate_sub_pop sum_rate_sub_pop
	tempfile sum
	save `sum', replace

	// Merge denominator
	restore
	merge m:1 location_id year_id using `sum', keep(3)nogen

	// Age-split at 1000 draw level
	forvalues i=0/999 {
		di in red "draw `i'"
		gen draw_`i'=rate_sub_pop*(tbhiv_d`i'/sum_rate_sub_pop)
		drop tbhiv_d`i' 
	}

	// Clean and save tempfile
	keep location_id year_id age_group_id sex_id draw_*
	tempfile hivtb_prev_cyas
	save `hivtb_prev_cyas', replace

	// Load back in TB all-form prevalence cases
	use `prev_cases', clear

	// Rename draws
	forvalues i = 0/999 {
		rename draw_`i' tb_`i'
	}

	// Merge the tb-all-forms and hivtb files
	merge 1:1 location_id year_id age_group_id sex using `hivtb_prev_cyas', keep(3) nogen 

	// Cap hivtb cases if hivtb/tb >90% of TB all forms
	forvalues i=0/999 {
		gen frac_`i'=draw_`i'/tb_`i'
		replace draw_`i'=tb_`i'*0.9 if frac_`i'>0.9 & frac_`i' !=.
		replace draw_`i'=0 if draw_`i'==.
	}

	// Clean
	drop tb_* frac_* m*
	tempfile hivtb_prev_capped
	save `hivtb_prev_capped', replace

	// Merge populations
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Convert to rate space
	forvalues i=0/999 {
		di in red "draw `i'"
		replace draw_`i'=draw_`i'/mean_pop
	}
				
	// Locations where the prevalence of HIV is zero (location_ids 161 and 186 for 1990) have missing draws, so replace them with zero
	foreach a of varlist draw_0-draw_999 {
		replace `a'=0 if `a'==.
	}

	// Clean HIV-TB prevalence file
	gen modelable_entity_id=1176
	gen measure_id=5
	tempfile hivtb_cyas_prev_capped

	// Save draws
	save `hivtb_cyas_prev_capped', replace
	save FILEPATH/HIVTB_prev_cyas_`model_version_id'_capped.dta, replace

	// Format for upload
	keep location_id year_id age_group_id sex_id draw_*

	// Prep for upload
	levelsof(location_id), local(ids) clean
		
	// Create csvs
	foreach location_id of local ids {
		qui outsheet if location_id==`location_id' using "FILEPATH/5_`location_id'.csv", comma replace
	}

**********************************************************************************************************************
** STEP 2C: COMPUTE HIV-TB INCIDENCE
**********************************************************************************************************************

	// Load incidence and population estimates 
	use `inc', clear
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Convert to case space
	forvalues i=0/999 {
		di in red "draw `i'"
		replace draw_`i'=draw_`i'*mean_pop
	}

	// Save tempfile and clean
	tempfile inc_cases
	save `inc_cases', replace
	keep if inlist(age_group_id, `ages')

	// Collapse draws by location_id and year_id
	fastcollapse draw_*, type(sum) by(location_id year_id) 

	// Merge on the fraction data
	merge 1:1 location_id year_id using "FILEPATH/Prop_tbhiv_mean_ui.dta", keepusing(mean_prop) keep(3)nogen
			
	// Apply HIV-TB proportions to TB no-HIV envelope
	forvalues i=0/999 {
		di in red "draw `i'"
		gen tbhiv_d`i'=mean_prop*draw_`i'
		drop draw_`i' 
	}

	// Prep for age split
	merge 1:m location_id year_id using `tmp_pop', keep(1 3) nogen
	merge m:1 location_id year_id age_group_id sex_id using `age_pattern', keep(3)nogen

	rename mean_pop sub_pop
	gen rate_sub_pop=rate*sub_pop

	// Compute denominator 
	preserve
	collapse (sum) rate_sub_pop, by(location_id year_id) fast
	rename rate_sub_pop sum_rate_sub_pop
	tempfile sum
	save `sum', replace

	// Merge denominator
	restore
	merge m:1 location_id year_id using `sum', keep(3)nogen

	// Apply age-split at 1000 draw level
	forvalues i=0/999 {
		di in red "draw `i'"
		gen draw_`i'=rate_sub_pop*(tbhiv_d`i'/sum_rate_sub_pop)
		drop tbhiv_d`i' 
	}

	// Clean and save tempfile
	keep location_id year_id age_group_id sex_id draw_*
	tempfile hivtb_inc_cyas
	save `hivtb_inc_cyas', replace

	// Load back in TB all-form incidence cases
	use `inc_cases', clear

	// Rename draws
	forvalues i = 0/999 {
		rename draw_`i' tb_`i'
	}

	// Merge the tb-all-forms and hivtb files
	merge 1:1 location_id year_id age_group_id sex using `hivtb_inc_cyas', keep(3) nogen 

	// Cap hivtb cases if hivtb/tb >90% of TB all forms
	forvalues i=0/999 {
		gen frac_`i'=draw_`i'/tb_`i'
		replace draw_`i'=tb_`i'*0.9 if frac_`i'>0.9 & frac_`i' !=.
		replace draw_`i'=0 if draw_`i'==.
	}

	// Clean and save tempfile
	drop tb_* frac_* m*
	tempfile hivtb_inc_capped
	save `hivtb_inc_capped', replace

	// Merge on populations
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Convert to rate space
	forvalues i=0/999 {
		di in red "draw `i'"
		replace draw_`i'=draw_`i'/mean_pop
	}
				
	// Locations where the prevalence of HIV is zero (location_ids 161 and 186 for 1990) have missing draws, so replace them with zero
	foreach a of varlist draw_0-draw_999 {
		replace `a'=0 if `a'==.
	}

	// Clean HIV-TB incidence file
	gen modelable_entity_id=1176
	gen measure_id=6
	tempfile hivtb_cyas_inc_capped

	// Save incidence draws
	save `hivtb_cyas_inc_capped', replace
	save FILEPATH/HIVTB_inc_cyas_`model_version_id'_capped.dta, replace

	// Clean
	use `hivtb_cyas_inc_capped', clear
	keep location_id year_id age_group_id sex_id draw_*

	// Save csvs
	levelsof(location_id), local(ids) clean
	foreach location_id of local ids {
		qui outsheet if location_id==`location_id' using "FILEPATH/6_`location_id'.csv", comma replace
	}
			
	// Upload HIV-TB to database
	run "FILEPATH/save_results_epi.ado"
	save_results_epi, input_dir(FILEPATH/`model_version_id') input_file_pattern({measure_id}_{location_id}.csv) modelable_entity_id(1176) description(`decomp_step': HIV-TB,`model_version_id'; HIV age-pattern - `hiv') measure_id(5 6) db_env("prod") decomp_step(`decomp_step') mark_best(True) clear 

**********************************************************************************************************************
** STEP 3: COMPUTE TB NO-HIV INCIDENCE & PREVALENCE
**********************************************************************************************************************

	// Pull populations
	clear all
	adopath + "FILEPATH"
	get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") decomp_step(`decomp_step') clear

	// Clean
	rename population mean_pop
	tempfile pop_all
	save `pop_all', replace
			
	// Load HIVTB incidence
	use FILEPATH/HIVTB_inc_cyas_`model_version_id'_capped.dta, clear
	keep location_id year_id age_group_id sex_id draw_* mean_pop

	// Convert to case space
	forvalues i = 0/999 {
		rename draw_`i' hivtb_`i'  
		replace hivtb_`i'=hivtb_`i'*mean_pop
	}

	// Save tempfile
	tempfile hivtb_inc
	save `hivtb_inc', replace

	// Load HIVTB prevalence
	use FILEPATH/HIVTB_prev_cyas_`model_version_id'_capped.dta, clear
	keep location_id year_id age_group_id sex_id draw_* mean_pop

	// convert to case space
	forvalues i = 0/999 {
		rename draw_`i' hivtb_`i'
		replace hivtb_`i'=hivtb_`i'*mean_pop
	}

	// Save tempfile
	tempfile hivtb_prev
	save `hivtb_prev', replace

	// bring in TB all forms
	use "FILEPATH/tb_`model_version_id'.dta", clear
	keep if measure_id==6

	// Clean
	duplicates drop location_id year_id age_group_id sex_id measure_id, force
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Convert to case space
	forvalues i=0/999 {
		di in red "draw `i'"
		replace draw_`i'=draw_`i'*mean_pop
	}

	// Merge HIV-TB incidence
	merge 1:1 location_id year_id age_group_id sex using `hivtb_inc', keep(3) nogen

	// Subtract HIV-TB from TB all-forms
	forvalues i=0/999 {
		replace draw_`i'=draw_`i'-hivtb_`i'
		replace draw_`i'=draw_`i'/mean_pop
	}

	// Clean and save
	drop mean_pop
	tempfile tb_noHIV_inc
	save `tb_noHIV_inc', replace

	// Now load in prevalence
	use "FILEPATH/tb_`model_version_id'.dta", clear
	keep if measure_id==5

	// Merge populations
	duplicates drop location_id year_id age_group_id sex_id measure_id, force
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Convert to case space
	forvalues i=0/999 {
		di in red "draw `i'"
		replace draw_`i'=draw_`i'*mean_pop
	}

	// Merge HIV-TB prevalence
	merge 1:1 location_id year_id age_group_id sex using `hivtb_prev', keep(3) nogen

	// Subtract HIV-TB from TB all-forms
	forvalues i=0/999 {
		replace draw_`i'=draw_`i'-hivtb_`i'
		replace draw_`i'=draw_`i'/mean_pop
	}

	// Clean and save
	drop mean_pop
	tempfile tb_noHIV_prev
	save `tb_noHIV_prev', replace

	// Append prevalence and incidence
	append using `tb_noHIV_inc'
	keep measure_id location_id year_id age_group_id sex_id draw_*
	gen modelable_entity_id=9969

	// Save draws
	save "FILEPATH/TBnoHIV_`model_version_id'.dta", replace 

	// Create prevalence data frame
	preserve
	keep if measure_id==5
	tempfile prev
	save `prev', replace

	// Create incidence data frame
	restore
	keep if measure_id==6
	tempfile inc
	save `inc', replace

	// Load in prevalence
	use `prev',clear
	levelsof(location_id), local(ids) clean

	// Save csvs for upload
	foreach location_id of local ids {
		qui outsheet if location_id==`location_id' using "FILEPATH/5_`location_id'.csv", comma replace
	}
			
	// Load in incidence
	use `inc',clear
	levelsof(location_id), local(ids) clean

	// Save csvs for upload
	foreach location_id of local ids {	
		qui outsheet if location_id==`location_id' using "FILEPATH/6_`location_id'.csv", comma replace
	}
		
	// Upload TB no-HIV to database
	run "FILEPATH/save_results_epi.ado"
	save_results_epi, input_dir("FILEPATH/`model_version_id'") input_file_pattern({measure_id}_{location_id}.csv) modelable_entity_id(9969) description(`decomp_step': TB no-HIV, `model_version_id') measure_id(5 6) db_env("prod") decomp_step(`decomp_step') mark_best(True) clear 



