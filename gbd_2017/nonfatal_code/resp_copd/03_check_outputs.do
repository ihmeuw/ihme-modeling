// (1) Checks to make sure all locations have saved successfully
// (2) Runs save_results

clear
set more off
local prefix "FILEPATH"
set odbcmgr unixodbc
local code "FILEPATH"
local stata_shell "FILEPATH/stata_shell.sh"

args output folder_name severity

//me_ids
if "`severity'"== "asympt" local me_id 3065
if "`severity'"== "mild" local me_id 1873
if "`severity'"== "moderate" local me_id 1874
if "`severity'"== "severe" local me_id 1875
	
// (1) CHECK OUTPUTS
	
	//Collect Locations to check
	quiet run "FILEPATH/get_location_metadata.ado"
	get_location_metadata, location_set_id(9) clear
	levelsof location_id, local(location_ids)

	//Get years to check
	quiet run "FILEPATH/get_demographics.ado"
	get_demographics, gbd_team(epi) clear
	local years `r(year_id)'
	di `years'
	
	//figure out which locations are missing, if any
	local completes 0
	local incomplete 0
	local mislocs
	local num 0

	foreach loc_id of local location_ids{
		foreach year of local years{
			foreach sex in 1 2{
						cap confirm file "FILEPATH/5_`loc_id'_`year'_`sex'.csv"
						if !_rc{
							local completes =`completes' +1
						}
						else {
							local mislocs `loc_id' `mislocs'
							local incomplete = `incomplete' + 1
						}
						local num = `num' +1
			} //close sex
		} //close year
	} //close location
	
	local mislocs : list uniq mislocs
	di in red "THESE ARE THE MISSING LOCATIONS `mislocs'"
	di in red "`completes' MANY FILES WROTE SUCCESSFULLY"
	di in red "`completes'/`num' finished"
	
// (2) SAVE RESULTS

	// get best model version, used in model description in save_results
	quiet run "FILEPATH/get_best_model_versions.ado"
	get_best_model_versions, entity(modelable_entity) ids(1872 3062 3063 3064) clear
	levelsof model_version_id, local(best_model) clean
	di `best_model'

	local filepattern "{measure_id}_{location_id}_{year_id}_{sex_id}.csv"

	save_results_epi, modelable_entity_id(`me_id') description("`severity' COPD model from `best_model' new splits") input_dir("FILEPATH") input_file_pattern(`filepattern') mark_best(True) clear

	
