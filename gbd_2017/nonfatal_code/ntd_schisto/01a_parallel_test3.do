// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Last updated:	03/17/17
// Description:	Parallelization of 01a_par, this pulls in draws of the unadjusted model, bins them into low, moderate, and high risk bins (according to WHO)
//				Then pulls in WHO population needing PC treatments, and divides by the proportion of the population
//				Create draws of population at risk


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


/*	local date 			2017_06_08
	local step_num 		01a
	local step_name		"par_adjustment"
	local location 		491
	local code_dir 		"FILEPATH"
	local in_dir 		"FILEPATH"
	local out_dir 		"FILEPATH"
	local tmp_dir 		"FILEPATH"
	local root_tmp_dir 	"FILEPATH"
	local root_j_dir 	"FILEPATH"
*/
	// write log if running in parallel and log is not already open
	cap log using "FILEPATH/`step_num'_`location'.smcl", replace
	if !_rc local close 1
	else local close 0

	// directory for standard code file
	adopath + FILEPATH
	adopath + FILEPATH

	// get demographics
    get_location_metadata, location_set_id(35) clear
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

	** put in manual fix for 44882 (Iran eliminated schistosomiasis in 2012) and 155 (Turkey eliminated in 2003)

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
	* pulls in unadjusted prevalence estimates from dismod
	* applies general geographic restrictions to all prevalence results
	* applies population at risk to get adjusted prevalence

	// pull in species-specific geographic exclusions
	import delimited "FILEPATH/species_specific_exclusions.csv", clear
	keep if location_id == `location'
	local keep = 1
	if haematobium != "" & mansoni != "" & japonicum != "" & other != "" {
		local keep = 0
	}


	di "pulling schisto unadjusted prevalence draws"
	// prevalence, all years, this location
	get_draws, gbd_id_type(modelable_entity_id) gbd_id(10537) source(ADDRESS) measure_id(5) location_id(`location') status(best) sex_id(1 2) clear
	replace measure_id = 5
	// clean files
	drop model_version_id modelable_entity_id
	tempfile draws
	save `draws', replace

	// if this is a universal geographic exclusion, replace draws with 0
	if `keep' == 0 {
		use `draws', clear
		forvalues i = 0/999 {
			qui replace draw_`i' = 0
		}
		// save file
		keep measure_id location_id year_id age_group_id sex_id draw_*
		export delimited "FILEPATH/`location'.csv", replace
	}

	if `keep' == 1 {
		** pull in population at risk estimates
		if inlist(location_id, 491,494,496,497,503,504,506,507,514,516,520,521) {
			use  "FILEPATH/prop_draws_chn_urbanmask.dta", clear
			keep if location_id == `location'
			merge 1:m location_id year_id using `draws', nogen keep(3)
			}
		else {
			use "FILEPATH/prop_draws_urbanmask.dta", clear
			keep if location_id == `location'
			merge 1:m location_id using `draws', nogen keep(3)
		}

		forvalues i = 0/999 {
			replace draw_`i' = draw_`i' * prop_`i'
		}

		** Turkey eliminated schisto in 2003
		if `location' == 155 {
			forvalues i = 0/999 {
				replace draw_`i' = 0 if year_id > 2002
			}
		}
		** Iran eliminated schisto in 2002
		if `location' == 44882 {
			forvalues i = 0/999 {
				replace draw_`i' = 0 if year_id > 2011
			}
		}
		if `location' == 206 {
			egen drawmean = rowmean(draw_*)
			sum  drawmean if year_id<2017
			local mean = `r(mean)'
			sum  drawmean if year_id==2017
			local mean2017 = `r(mean)'
			forvalues i = 0 / 999 {
				replace draw_`i' = draw_`i' * `mean' / `mean2017' if year_id==2017
			}
		}

		** save file
		keep measure_id location_id year_id age_group_id sex_id draw_*
		export delimited "FILEPATH/`location'.csv", replace
	}


** write check here
	file open finished using "FILEPATH/finished_loc`location'.txt", replace write
	file close finished

** close logs
	if `close' log close
	clear
