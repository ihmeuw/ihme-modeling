**Idiopathic Split: Split dismod results into idiopathic and secondary epilepsy and save draws
**GBD 2017 

//prep stata 
clear all
set more off
set maxvar 32000

// Set OS flexibility 
	if c(os) == "Unix" {
		local j "FILEPATH"
		set odbcmgr unixodbc
		local h "FILEPATH"
	}
	else if c(os) == "Windows" {
		local j "FILEPATH"
		local h "FILEPATH"
	}

qui do "`j'/FILEPATH/get_draws.ado"

//transfer locals
args code_folder save_folder_ratio loc 

// STEP 1: get prevalence and incidence draw level results
get_draws, gbd_id_type(modelable_entity_id) gbd_id(16406) location_id(`loc') measure_id(5 6) source(epi) clear

// Step 2: calculcate mean value for age/sex/year
egen mean = rmean(draw_*)
keep age_group_id year_id sex_id measure_id modelable_entity_id model_version_id location_id mean

// Step 3: reshape 
reshape wide mean, i(age_group_id year_id sex_id modelable_entity_id model_version_id location_id) j(measure_id)

gen ratio = mean6 / mean5

export delim using "`save_folder_ratio'ratio_`loc'.csv", replace 
