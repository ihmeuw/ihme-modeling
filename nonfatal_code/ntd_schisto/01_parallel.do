// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		USERNAME
// Last updated:	DATE
// Description:	Parallelization of 01_species_split

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "J:"
	}

// define locals from qsub command
	local location 		`1'
	
	local outDir FILEPATH

*/
	// write log if running in parallel and log is not already open
	capture log close
	log using "FILEPATH/01_`location'.smcl", replace


	// directory for standard code file
	adopath + FILEPATH
	adopath + FILEPATH

	// get demographics
    get_location_metadata, location_set_id(9) clear
    keep if most_detailed == 1 & is_estimate == 1
    keep location_id region_name
    tempfile location_data
    save `location_data', replace

	local years 1990 1995 2000 2005 2010 2016
	local sexes 1 2


	// species types
	local species "haematobium mansoni japonicum other"

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
** this code first...
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
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(2797) source(epi) measure_ids(5) location_ids(`location') status(best) sex_ids(1 2) clear

	// clean files
	drop model_version_id modelable_entity_id

	// if this is a universal geographic exclusion, replace draws with 0
	if `keep' == 0 {
		forvalues i = 0/999 {
			qui replace draw_`i' = 0
		}
		// save file
		keep measure_id location_id year_id age_group_id sex_id draw_*
		export delimited "FILEPATH/`location'.csv", replace
		export delimited "FILEPATH/`location'.csv", replace
	}

	// if this is not a universal geographic exclusion, continue on
	if `keep' == 1 {
		keep measure_id location_id year_id age_group_id sex_id draw_*

		forvalues i = 0/999 {
			replace draw_`i' = 0 if age_group_id < 5 | age_group_id == 164
		}


		if `haem' == 1 {
			** export values for haematobium
			export delimited "FILEPATH/`location'.csv", replace
			forvalues i = 0/999 {
				replace draw_`i' = 0
			}
			** save 0s as mansoni
			export delimited "FILEPATH/`location'.csv", replace
		}
		if `mans' == 1 {
			** save values as mansoni
			export delimited "FILEPATH/`location'.csv", replace
			forvalues i = 0/999 {
				replace draw_`i' = 0
			}
			** save 0s as haematobium
			export delimited "FILEPATH/`location'.csv", replace
		}

		// check to see if this country needs to be split by haematobium and mansoni
		if `split' == 1 {
			merge m:1 location_id using `location_data', nogen keep(3)
			levelsof(region_name), local(region)
			preserve
			// bring in glm species split results
			import delimited "FILEPATH/coinfection_region_splits.csv", clear
			keep if region_name == `region'
			forvalues i = 0/999 {
				qui gen draw_h_`i' = rnormal(prop_h, se_h)
				replace draw_h_`i' = 1 if draw_h_`i' > 1
				replace draw_h_`i' = 0 if draw_h_`i' < 0
				qui gen draw_m_`i' = rnormal(prop_m, se_m)
				replace draw_m_`i' = 1 if draw_m_`i' > 1
				replace draw_m_`i' = 0 if draw_m_`i' < 0
				gen total_`i' = draw_h_`i' + draw_m_`i'
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
	file open finished using "FILEPATH/`location'.txt", replace write
	file close finished

// close logs
	if `close' log close
	clear

	