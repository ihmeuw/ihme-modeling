// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author: 	
// Date: 		04 August 2014
// Updated:
// Purpose:	Apply scalar to Herpes to account for undiagnosed

// PREP STATA
	clear all
	set more off
	// SET OS FLEXIBILITY

// ****************************************************************************
// Manually defined macros
	** User
	local username USER

	** Steps to run (0/1)
	local apply_scalar 0
	local upload 1
	local sweep 0

	local prog_dir "PROG_DIR"

// ****************************************************************************
// Automated macros

	local tmp_dir "TMP_DIR"
	capture mkdir "`tmp_dir'"
	capture mkdir "`tmp_dir'/02_newlogs"
	capture mkdir "`tmp_dir'/01_draws"
	capture mkdir "`tmp_dir'/errors"
	capture mkdir "`tmp_dir'/outputs"

	run "FILEPATH/get_best_model_versions.ado"
	run "FILEPATH/get_location_metadata.ado"
// ****************************************************************************
// Load original models

	local me_id 1642
	local parent_model = 299651
	dis "`parent_model'"



// ****************************************************************************
// Get country list
	get_location_metadata, location_set_id(35) clear
	levelsof location_id if most_detailed==1, local(locations)

// ****************************************************************************
// Modify draws to apply undiagnosed scalar?
	if `apply_scalar' == 1 {
		//create the undiagnosed scalar draws
		clear
		local M = 0.01007
		local SE = 0.0055
		local N = `M'*(1-`M')/`SE'^2
		local a = `M'*`N'
		local b = (1-`M')*`N'
		clear
		set obs 1000
		gen prop_ = rbeta(`a',`b')
		gen num = _n-1
		gen mvar = 1
		reshape wide prop_, i(mvar) j(num)
		saveold "`tmp_dir'/prop_draws.dta", replace
		foreach loc of local locations {
			!qsub -P proj_custom_models -pe multi_slot 4 -l mem_free=8g -N "herpes_scalar_`loc'" -o "`tmp_dir'/outputs" -e "`tmp_dir'/errors" "`prog_dir'/stata_shell.sh" "`prog_dir'/01_apply_scalar.do" "`tmp_dir' `parent_model' `loc'"
		}
	}

// ****************************************************************************

	if `upload' == 1 {
			run "FILEPATH/save_results_epi.ado"
			save_results_epi, input_dir("`tmp_dir'/01_draws") input_file_pattern("{measure_id}_{location_id}_{year_id}_{sex__id}.csv") modelable_entity_id(3090) description("17.5 percent of parent incidence, 3 week duration applied to model `parent_model'")  mark_best("True") clear
			noisily di "UPLOADED -> " c(current_time)
	}

// ****************************************************************************
// Clear draws?
	if `sweep' == 1 {
		!rm -rf "`tmp_dir'"
	}

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
