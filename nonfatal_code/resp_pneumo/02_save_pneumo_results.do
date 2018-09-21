//A checker to make sure all of the jobs finished, save results

//prepare stata
	clear
	set more off
	
	// load in shared function for save_results
	adopath + "FILEPATH"
	
	args output ver_desc
	local coal_workers_ac 1893
	local coal_workers_end 3052

	get_best_model_versions, entity(modelable_entity) ids(`coal_workers_ac') clear
	levelsof model_version_id, local(best_model) clean	
		
	di "SAVING RESULTS"
	save_results, modelable_entity_id(`coal_workers_end') description("Pneumo model `best_model' with proper zeros") file_pattern("{measure_id}_{location_id}_{year_id}_{sex_id}.csv") in_dir("FILEPATH") mark_best("yes")
	
	

	
