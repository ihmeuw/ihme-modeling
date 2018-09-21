// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:	Apply pregnagncy adjustment to parent PMS model


// PREP STATA
	clear all
	set more off
	set maxvar 3200
	if c(os) == "Unix" {
		global prefix "{FILEPATH}"
		set odbcmgr unixodbc
		set mem 2g
	}
	else if c(os) == "Windows" {
		global prefix "{FILEPATH}"
		set mem 2g
	}

	run "{FILEPATH}/get_best_model_versions.ado"
	run "{FILEPATH}/get_location_metadata.ado"
// ****************************************************************************
// Manually defined macros
	** User
	local username {USERNAME}

	** Steps to run (0/1)
	local apply_proportion 1
	local upload 1
	local sweep 0


	local prog_dir "{FILEPATH}"

// ****************************************************************************
// Automated macros

	local tmp_dir "{FILEPATH}"
	capture mkdir "`tmp_dir'"
	capture mkdir "`tmp_dir'/00_logs"
	capture mkdir "`tmp_dir'/01_draws"

// ****************************************************************************
// Load original parent models
	local pms_id {MODELABLE ENTITY ID}
	local prop_id {MODELABLE ENTITY ID}
	local final_pms_id {MODELABLE ENTITY ID}
	get_best_model_versions, entity(modelable_entity) ids(`pms_id' `prop_id') clear
	levelsof model_version_id if modelable_entity_id == `pms_id', local(cases_model)
	levelsof model_version_id if modelable_entity_id == `prop_id', local(prop_model)

// ****************************************************************************
// Get country list
	get_location_metadata, location_set_id({LOCATION SET ID}) clear
	levelsof location_id if most_detailed=={MOST DETAILED}, local(locations)

// ****************************************************************************
// Modify draws to apply pregnancy proportion?
	if `apply_proportion' == 1 {
		foreach loc of local locations {
			!qsub -P proj_custom_models -pe multi_slot 4 -l mem_free=8g -e "/dev/null" -o "/dev/null" -N "PMS_proportion_`loc'" "`prog_dir'/stata_shell.sh" "`prog_dir'/01_apply_proportion.do" "`tmp_dir' `loc' `pms_id' `prop_id'"
		}
	}

// ****************************************************************************
// Upload?
	if `upload' == 1 {
	    if `apply_proportion' == 1 {
	        sleep 6000000
	        }
		quietly {
			run {FILEPATH}/save_results.do
			save_results, modelable_entity_id(`final_pms_id') metrics(incidence prevalence) description("model `case_model' reduced by model `prop_model'") in_dir("`tmp_dir'/01_draws") mark_best("yes")
			noisily di "UPLOADED -> " c(current_time)
		}
	}

// ****************************************************************************
// Clear draws?
	if `sweep' == 1 {
		!rm -rf "`tmp_dir'"
	}

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
