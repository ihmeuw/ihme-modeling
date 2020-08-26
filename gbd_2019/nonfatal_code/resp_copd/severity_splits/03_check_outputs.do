// (1) Checks to make sure all locations have saved successfully
// (2) Runs save_results

// SETUP

disp("Starting Checking Outputs --------------------------------------")
quiet run "FILEPATH/save_results_epi.ado"
run "FILEPATH/get_best_model_versions.ado"
//prepare stata
	clear
	set more off

// Define J drive (data) for cluster (UNIX) and Windows (Windows)
	if c(os) == "Unix" {
		local prefix "FILEPATH"
		set odbcmgr unixodbc
		local code "FILEPATH"
		local stata_shell "FILEPATH"
	}
	else if c(os) == "Windows" {
		local prefix "FILEPATH"
		local code "FILEPATH"
	}

// load in shared functions
	adopath + `prefix'FILEPATH
	qui{
		do "FILEPATH/get_best_model_versions.ado"
		do "FILEPATH/save_results_epi.ado"
	}

///////////////////////////
// READ IN ARGUMENTS
///////////////////////////

	args output2 folder_name severity
	di "`output2' `folder_name' `severity'"

	//me_ids
	if "`severity'"== "asympt" local me_id 3065
	if "`severity'"== "mild" local me_id 1873
	if "`severity'"== "moderate" local me_id 1874
	if "`severity'"== "severe" local me_id 1875

////////////////////////////
// (1) CHECK OUTPUTS
////////////////////////////

	//Collect Locations to check
	quiet run "FILEPATH/get_location_metadata.ado"
	get_location_metadata, location_set_id(35) gbd_round_id(6) clear
	keep if most_detailed==1
	levelsof location_id, local(location_ids)

	//Get years to check
	adopath + "FILEPATH"
	get_demographics, gbd_team(epi) gbd_round_id(6) clear
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
						cap confirm file "`output2'/`folder_name'/`me_id'/5_`loc_id'_`year'_`sex'.csv"
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
	get_best_model_versions, entity(modelable_entity) ids(24543 3062 3063 3064) gbd_round_id(6) decomp_step("step4") clear
	levelsof model_version_id, local(best_model) clean
	di `best_model'

	local filepattern "{measure_id}_{location_id}_{year_id}_{sex_id}.csv"

	save_results_epi, modelable_entity_id(`me_id') description("`severity' COPD model from `best_model' REs EMR Default") input_dir("`output2'/`folder_name'/`me_id'/") input_file_pattern(`filepattern') mark_best(False) gbd_round_id(6) decomp_step("step4") clear
