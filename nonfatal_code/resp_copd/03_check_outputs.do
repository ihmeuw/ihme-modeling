// (1) Checks to make sure all locations have saved successfully
// (2) Runs save_results

////////////////////////////
// SETUP
////////////////////////////

//prepare stata
	clear
	set more off
	
// load in shared functions
	adopath + "PATH"
	qui{
		do "PATH/get_best_model_versions.ado"
		do "PATH/save_results.do"
	}

//me_ids
	local asympt 3065
	local mild 1873
	local moderate 1874
	local severe 1875

///////////////////////////
// READ IN ARGUMENTS
///////////////////////////

	args output folder_name
	di "`output' `folder_name'"
	
////////////////////////////
// (1) CHECK OUTPUTS
////////////////////////////
	
	//Collect Locations to check
	quiet run "PATH/get_location_metadata.ado"
	get_location_metadata, location_set_id(9) clear
	levelsof location_id, local(location_ids)
	
	//figure out which locations are missing, if any
	local completes 0
	local incomplete 0
	local mislocs
	local num 0
	local years 1990 1995 2000 2005 2010 2016
	foreach loc_id of local location_ids{
		foreach year of local years{
			foreach sex in 1 2{
						cap confirm file "FILEPATH.csv"
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
	
/////////////////////////
// (2) SAVE RESULTS
/////////////////////////

	// get best model version, used in model description in save_results
	get_best_model_versions, entity(modelable_entity) ids(1872 3062 3063 3064) clear
	levelsof model_version_id, local(best_model) clean
	di `best_model'

	save_results, modelable_entity_id(3065) description("Asympt COPD model from `best_model'") in_dir("PATH/`folder_name'/FILEPATH/") mark_best(yes)
	save_results, modelable_entity_id(1873) description("Mild COPD model from `best_model'") in_dir("PATH/`folder_name'/FILEPATH/") mark_best(yes)
	save_results, modelable_entity_id(1874) description("Moderate COPD model from `best_model'") in_dir("PATH/`folder_name'/FILEPATH/") mark_best(yes)
	save_results, modelable_entity_id(1875) description("Severe COPD model from `best_model'") in_dir("PATH/`folder_name'/FILEPATH/") mark_best(yes)

	
