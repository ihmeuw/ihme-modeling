// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
// Purpose:		Incoporates retreated cases to incidence estimates for HIV-TB and TB no-HIV
// Author:		USERNAME
// Edited:      USERNAME
// Description:	Pulls ST-GPR draws with the proportion of retreated cases and incorporates them into the TB parent
//				Re-computes TB no-HIV and HIV-TB with retreated cases as in first script
// 				saves HIV-TB and TB no-HIV envelopes
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
	local version 478688
	local model_version_id dismod_478688_ret // me_id=9422 (dismod)
	local decomp_step step4
	
**********************************************************************************************************************
** STEP 1A: GATHER INPUTS (TB INCIDENCE DRAWS AND POPULATIONS) FOR COMPUTATIONS
**********************************************************************************************************************

	// Pull populations
	adopath + "FILEPATH"
	get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") decomp_step(`decomp_step') gbd_round_id(6) clear

	// Clean populations
	rename population mean_pop
	tempfile pop_all
	save `pop_all', replace
			
	// Keep sex-specific populations
	use `pop_all', clear
	drop if year_id<1980
	drop if location_id==1
	drop if sex_id==3
	tempfile tmp_pop
	save `tmp_pop', replace


	// Get retreated proportions
	use "FILEPATH/tb_retreated_prop_draws_`decomp_step'.dta"

	// Rename draws
	forvalues i = 0/999 {
		rename draw_`i' ret_`i'
		replace ret_`i'=0.9 if ret_`i'>=0.9
	}

	// Create tempfile
	tempfile ret_prop
	save `ret_prop', replace
				
	// Pull incidence 			
	use "FILEPATH/tb_dismod_`version'.dta", clear		
	keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
	keep if measure_id==6

	// Merge populations
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Convert to case space
	forvalues i=0/999 {
		di in red "draw `i'"
		replace draw_`i'=draw_`i'*mean_pop
	}

	// Clean incidence data
	drop mean_pop
	tempfile inc_tmp
	save `inc_tmp', replace

**********************************************************************************************************************
** STEP 1B: INCORPORATE RETREATED CASES TO ESTIMATED INCIDENT CASES
**********************************************************************************************************************	

	// Merge on retreated proportions
	merge m:1 location_id year_id using `ret_prop', keep(3)nogen

	// Compute retreated cases: ret_cases=(ret_prop*inc)/(1-ret_prop)
	forvalues i=0/999 {
		di in red "draw `i'"
		replace draw_`i'=(draw_`i'*ret_`i')/(1-ret_`i')
	}

	// Clean retreated data set
	keep location_id year_id age_group_id sex_id draw_*
	tempfile ret
	save `ret', replace

	// Append incidence
	append using `inc_tmp'

	// Sum incident cases and retreated cases at the draw level
	fastcollapse draw_*, type(sum) by(location_id year_id age_group_id sex_id) 

	// Convert to rate space
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Do conversion in draw space
	forvalues i=0/999 {
		di in red "draw `i'"
		replace draw_`i'=draw_`i'/mean_pop
	}

	// Save complete data set
	save "FILEPATH/tb_`model_version_id'.dta", replace 

**********************************************************************************************************************
** STEP 2: SPLIT RETREATED CASES INTO HIV-TB 
**********************************************************************************************************************	
	
	// Read retreated draws
	use "FILEPATH/tb_`model_version_id'.dta", clear
	keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
	tempfile inc
	save `inc', replace

	// Get HIV prev age pattern
	use "FILEPATH/hiv_prev_age_pattern.dta", clear
	drop model_version_id
	keep if measure_id==5
	keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
	rename mean rate
	drop lower upper
	tempfile age_pattern
	save `age_pattern', replace

	// Merge populations
	use `inc', clear
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Convert to cases
	forvalues i=0/999 {
		di in red "draw `i'"
		replace draw_`i'=draw_`i'*mean_pop
	}

	// Save a tempfile
	tempfile inc_cases
	save `inc_cases', replace

	// Collapse draws to loc-year level
	keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
	fastcollapse draw_*, type(sum) by(location_id year_id) 

	// Merge on the fraction data
	merge 1:1 location_id year_id using "FILEPATH/Prop_tbhiv_mean_ui.dta", keepusing(mean_prop) keep(3)nogen
			
			// Apply fractions
			forvalues i=0/999 {
				di in red "draw `i'"
				gen tbhiv_d`i'=mean_prop*draw_`i'
				drop draw_`i' 
			}

	// Prep for age-split
	merge 1:m location_id year_id using `tmp_pop', keep(1 3) nogen
	merge m:1 location_id year_id age_group_id sex_id using `age_pattern', keep(3)nogen

	rename mean_pop sub_pop
	gen rate_sub_pop=rate*sub_pop

	// Compute proportions for split
	preserve
	collapse (sum) rate_sub_pop, by(location_id year_id) fast
	rename rate_sub_pop sum_rate_sub_pop
	tempfile sum
	save `sum', replace

	// Merge on proportions
	restore
	merge m:1 location_id year_id using `sum', keep(3)nogen

	// Compute age-specific incidence
	forvalues i=0/999 {
		di in red "draw `i'"
		gen draw_`i'=rate_sub_pop*(tbhiv_d`i'/sum_rate_sub_pop)
		drop tbhiv_d`i' 
	}

	// Clean draws
	keep location_id year_id age_group_id sex_id draw_*
	tempfile hivtb_inc_cyas
	save `hivtb_inc_cyas', replace

**********************************************************************************************************************
** STEP 3: CAP HIV-TB CASES IF HIVTB/TB-ALL > 90% 
**********************************************************************************************************************	
	
	// Pull incidence all
	use `inc_cases', clear

	// Rename draws
	forvalues i = 0/999 {
		rename draw_`i' tb_`i'
	}

	// Merge the tb-all-forms and hivtb files
	merge 1:1 location_id year_id age_group_id sex using `hivtb_inc_cyas', keep(3) nogen 

	// loop through draws and adjust them
	forvalues i=0/999 {
		gen frac_`i'=draw_`i'/tb_`i'
		replace draw_`i'=tb_`i'*0.9 if frac_`i'>0.9 & frac_`i' !=.
		replace draw_`i'=0 if draw_`i'==.
	}

	// Save fianl retreated HIVTB cases			
	drop tb_* frac_* m*
	gen measure_id=6
	save FILEPATH/HIVTB_inc_cases_`model_version_id'_capped.dta, replace

**********************************************************************************************************************
** STEP 4: COMPUTE RETREATED CASES FOR TB NO-HIV
**********************************************************************************************************************	
	
	// Clean HIVTB draws	
	keep location_id year_id age_group_id sex_id draw_* 

	// Rename draws
	forvalues i = 0/999 {
		rename draw_`i' hivtb_`i'  
	}

	// Save tempfile
	tempfile hivtb_inc
	save `hivtb_inc', replace

	// Bring in TB all forms
	use "FILEPATH/tb_`model_version_id'.dta", clear
	merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

	// Convert to cases
	forvalues i=0/999 {
		di in red "draw `i'"
		replace draw_`i'=draw_`i'*mean_pop
	}

	// Merge on HIVTB cases
	merge 1:1 location_id year_id age_group_id sex using `hivtb_inc', keep(3) nogen

	// Compute TB no-HIV by subtracting HIVTB cases
	forvalues i=0/999 {
		replace draw_`i'=draw_`i'-hivtb_`i'
	}

	// Clean
	drop mean_pop
	keep location_id year_id age_group_id sex_id draw_*

	// Save
	gen measure_id=6
	save "FILEPATH/TBnoHIV_inc_cases_`model_version_id'.dta", replace 

