// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Last updated:	01/18/2017
// Description:	Parallelization of 01_scale_par

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}

// define locals from qsub command
	local date 			`1'
	local step_num 		`2'
	local step_name		`3'
	local location 		`4'
	local code_dir 		`5'
	local in_dir 		`6'
	local out_dir 		`7'
	local tmp_dir 		`8'
	local root_tmp_dir 	`9'
	local root_j_dir 	`10'

	// write log if running in parallel and log is not already open
	cap log using "`tmp_dir'/02_temp/02_logs/`step_num'_`location'.smcl", replace
	if !_rc local close 1
	else local close 0

	// directory for standard code file
	adopath + FILEPATH
	adopath + FILEPATH

	// get demographics
    get_location_metadata, location_set_id(9) clear
    keep if most_detailed == 1 & is_estimate == 1
    keep location_id region_name
    tempfile location_data
    save `location_data', replace

    // get demographics
   	/*get_demographics, gbd_team(ADDRESS) clear
	local years = "$year_ids"
	local sexes = "$sex_ids" */
	local years 1990 1995 2000 2005 2010 2017
	local sexes 1 2

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
** this code adjusts for geographic restrictions and proportion of the population at risk
	* pulls in unadjusted prevalence estimates from dismod
	* applies geographic restrictions to all prevalence results
	* applies population at risk to get adjusted prevalence

	// get draws from schisto unadjusted prevalence model
	di "pulling lf unadjusted prevalence draws"
	// prevalence, all years, this location
	get_draws, gbd_id_type(modelable_entity_id) gbd_id(1491) source(ADDRESS) measure_id(5) location_id(`location') status(best) sex_id(1 2) clear

	preserve
	// pull in species-specific geographic exclusions
	import delimited "$prefix/WORK/12_bundle/ntd_lf/gbd2017/02_inputs/geographic_exclusions.csv", clear
	keep if location_id == `location'
	tempfile restrictions
	save `restrictions', replace
	restore

	// merge in restrictions
	merge m:1 location_id year_id using `restrictions', nogen keep(3)
	// multiply by include (replaces draws with 0s if they are excluded)
	forvalues i = 0/999 {
		qui replace draw_`i' = draw_`i' * include
	}

	// pull in proportion of the population at risk
	preserve
	import delimited "FILEPATH/prop_pop_at_risk_2017_ammended.csv", clear
	keep if location_id == `location' // this takes care of applying what was calculated as national estimates to the subnational
	tempfile prop
	save `prop', replace
	restore
	merge m:1 location_id year_id using `prop', nogen keep(1 3)
	// adjust draws for proportion of the population at risk
	forvalues i = 0/999 {
		qui replace draw_`i' = draw_`i' * prop_at_risk
		qui replace draw_`i' = 0 if draw_`i' == .
	}

	// clean it
	keep measure_id location_id year_id age_group_id sex_id draw_*

	// save it
	export delimited "FILEPATH/`location'.csv", replace

// write check here
	file open finished using "FILEPATH/finished_loc`location'.txt", replace write
	file close finished

// close logs
	if `close' log close
	clear
