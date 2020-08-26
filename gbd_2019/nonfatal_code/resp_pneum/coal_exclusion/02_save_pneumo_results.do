//A checker to make sure all of the jobs finished, save results

//prepare stata
	clear
	set more off

// Define J drive (data) for cluster (UNIX) and Windows (Windows)
	if c(os) == "Unix" {
		local prefix "FILEPATH"
		local prefix_h "FILEPATH"
		set odbcmgr unixodbc
		local code "`prefix_h'FILEPATH"
		local stata_shell "`prefix_h'FILEPATH"
	}
	else if c(os) == "Windows" {
		local prefix "FILEPATH"
		local code "`prefix'FILEPATH"
	}

	qui do `prefix'FILEPATH/save_results_epi.ado
	qui do `prefix'FILEPATH/get_best_model_versions.ado

	args output ver_desc
	local coal_workers_ac 24658 //10/4/2019 changed to EMR bundle, previously 1893
	local coal_workers_end 3052

	//// CHECK OUTPUTS -------------------------------------------------------------------------------

	//Collect Locations to check
	quiet run "FILEPATH/get_location_metadata.ado"
	get_location_metadata, location_set_id(9) gbd_round_id(6) clear
	levelsof location_id, local(location_ids)

	//Get years to check
	adopath + "FILEPATH"
	get_demographics, gbd_team(epi) gbd_round_id(6) clear
	local years `r(year_id)'
	di `years'

	//figure out which locations are missing, if any
	local completes 0
	local incomplete 0
	local mislocs 0
	local num 0

	foreach loc_id of local location_ids{
		foreach year of local years{
			foreach sex in 1 2{
						//cap confirm file "`output'/`ver_desc'/`coal_workers_end'/5_`loc_id'_`year'_`sex'.csv"
						cap confirm file "`output'/5_`loc_id'_`year'_`sex'.csv"
						*// cap confirm file "`output'/`ver_desc'/3052/5_`loc_id'_`year'_`sex'.csv"
						if !_rc{
							local completes =`completes' +1
						}
						else {
							local mislocs `loc_id' `mislocs'
							local incomplete = `incomplete' + 1
						}
						local num = `num' +1
			}
		}
	}

	local mislocs : list uniq mislocs
	di in red "THESE ARE THE MISSING LOCATIONS `mislocs'"
	di in red "`completes' MANY FILES WROTE SUCCESSFULLY"
	di in red "`completes'/`num' finished"

	/// SAVE RESULTS -------------------------------------------------------------------------------------


	get_best_model_versions, entity(modelable_entity) ids(`coal_workers_ac') gbd_round_id(6) decomp_step("step4") clear
	levelsof model_version_id, local(best_model) clean

	di "SAVING RESULTS"
	save_results_epi, modelable_entity_id(`coal_workers_end') description("Coal pneumo model 2019 decomp4") input_file_pattern("{measure_id}_{location_id}_{year_id}_{sex_id}.csv") input_dir("`output'") mark_best(True) gbd_round_id(6) decomp_step("step4") clear
