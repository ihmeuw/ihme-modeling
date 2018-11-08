// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Last updated:	01/18/2017
// Description:	Parallelization of 01_species_split
// do "FILEPATH"
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

	local loc `location'

/*	local date 			2017_06_22
	local step_num 		01
	local step_name		"species_split"
	local location 		175
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


	// species types
	local species "haematobium mansoni japonicum other"

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
	* pulls in unadjusted prevalence estimates from dismod
	* applies general geographic restrictions to all prevalence results
	* applies population at risk to get adjusted prevalence
	* then splits out countries that have both haematobium and mansoni by species using coinfection region splits from glm model

	// pull in species-specific geographic exclusions
	import delimited "FILEPATH/species_specific_exclusions.csv", clear
	keep if location_id == `location'

	// drop if it is in all 3 species exclusions
	local keep = 1
	if haematobium != "" & mansoni != "" & japonicum != "" & other != "" {
		local keep = 0
	}

	// does this location need to be split across species?
	local split = 0
	if haematobium == "" & mansoni == "" {
		local split = 1
	}

	local haem = 0
	local mans = 0
	if `split' == 0 & `keep' == 1 {
		if haematobium == "" {
			local haem = 1
		}
		else if haem != "" {
			local mans = 1
		}
	}

	// get draws from schisto unadjusted prevalence model
	di "pulling schisto adjusted prevalence draws"
	// prevalence, all years, this location
	get_draws, gbd_id_type(modelable_entity_id) gbd_id(2797) source(ADDRESS) measure_id(5) location_id(`location') status(best) sex_id(1 2) clear
	// clean files
	drop model_version_id modelable_entity_id

	// if this is a universal geographic exclusion, replace draws with 0
	if `keep' == 0 {
		forvalues i = 0/999 {
			qui replace draw_`i' = 0
		}
		// save file
		keep measure_id location_id year_id age_group_id sex_id draw_*
		export delimited "`tmp_dir'/draws/2965/`location'.csv", replace
		export delimited "`tmp_dir'/draws/2966/`location'.csv", replace
	}

	// if this is not a universal geographic exclusion, continue on
	if `keep' == 1 {
		*import delimited "`FILEPATH/`location'.csv", clear
		keep measure_id location_id year_id age_group_id sex_id draw_*

		forvalues i = 0/999 {
			replace draw_`i' = 0 if age_group_id < 5 | age_group_id == 164
		}


		if `haem' == 1 {
			** export values for haematobium
			export delimited "`tmp_dir'/draws/2965/`location'.csv", replace
			forvalues i = 0/999 {
				replace draw_`i' = 0
			}
			** save 0s as mansoni
			export delimited "`tmp_dir'/draws/2966/`location'.csv", replace
		}
		if `mans' == 1 {
			** save values as mansoni
			export delimited "`tmp_dir'/draws/2966/`location'.csv", replace
			forvalues i = 0/999 {
				replace draw_`i' = 0
			}
			** save 0s as haematobium
			export delimited "`tmp_dir'/draws/2965/`location'.csv", replace
		}

		// check to see if this country needs to be split by haematobium and mansoni
		if `split' == 1 {
			// get region name in there
			merge m:1 location_id using `location_data', nogen keep(3)
			levelsof(region_name), local(region)
			preserve
			// bring in glm species split results
			import delimited "`out_dir'/coinfection_region_splits.csv", clear
			keep if region_name == `region'
			forvalues i = 0/999 {
				qui gen draw_h_`i' = rnormal(prop_h, se_h)
				replace draw_h_`i' = 1 if draw_h_`i' > 1
				replace draw_h_`i' = 0 if draw_h_`i' < 0
				qui gen draw_m_`i' = rnormal(prop_m, se_m)
				replace draw_m_`i' = 1 if draw_m_`i' > 1
				replace draw_m_`i' = 0 if draw_m_`i' < 0
				gen total_`i' = draw_h_`i' + draw_m_`i'
				** adjust to make sure the sum of each pair of draws is at least 1
				replace draw_h_`i' = draw_h_`i' * (1/total_`i') if total_`i' < 1
				replace draw_m_`i' = draw_m_`i' * (1/total_`i') if total_`i' < 1
			}
			gen location_id = `location'
			tempfile prop_draws
			save `prop_draws', replace
			// pull in prevalence draws
			restore
			// multiply draws to get species-specific prevalence
			merge m:1 location_id using `prop_draws', nogen keep(3)
			forvalues i = 0/999 {
				qui replace draw_h_`i' = draw_`i' * draw_h_`i'
				qui replace draw_m_`i' = draw_`i' * draw_m_`i'
				qui drop draw_`i'
			}
			// save species-specific draws by location
			preserve
			drop draw_m* se_* region_name
			export delimited "FILEPATH/haematobium_`location'.csv", replace
			rename draw_h_* draw_*
			keep measure_id location_id year_id age_group_id sex_id draw_*
			export delimited "FILEPATH/`location'.csv", replace
			restore, preserve
			drop draw_h* se_* region_name
			export delimited "FILEPATH/mansoni_`location'.csv", replace
			rename draw_m_* draw_*
			keep measure_id location_id year_id age_group_id sex_id draw_*
			export delimited "FILEPATH/`location'.csv", replace
			restore
		}

	}

// write check here
	file open finished using "FILEPATH/finished_loc`location'.txt", replace write
	file close finished

// close logs
	if `close' log close
	clear
