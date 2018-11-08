// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		
// Last updated:	2/9/2016
// Description:	Parallelization of 02a_cfr_draws
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

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


// define other locals
	// directory for standard code files
	adopath + "FILEPATH"
	// functional
	local functional "encephalitis"

	// get locals from demographics
	get_demographics, gbd_team(epi) clear
	local years = r(year_id)
	local sexes = r(sex_id)
	local age_group_ids = r(age_group_id)

// write log if running in parallel and log is not already open
	cap log using "FILEPATH", replace
	if !_rc local close 1
	else local close 0

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
run /home/j/temp/central_comp/libraries/current/stata/get_draws.ado
// get draws from most recent best DisMod model for location (remission (7) and excess mortality (9)) //CHANGE THIS TO BEST!!!
	get_draws, gbd_id_type(modelable_entity_id) gbd_id(1419) measure_id(7 9) location_id(`location') source(epi) gbd_round_id(5) status(best) age_group_id(`age_group_ids') clear
	//drop if age_group_id >= 22
	drop model_version_id

	foreach year of local years {
		foreach sex of local sexes {
			preserve
			keep if year_id == `year' & sex_id == `sex' & measure_id == 7
			drop measure_id
			forvalues c = 0/999 {
				qui rename draw_`c' rem_`c' // renaming draws to remission
			}
			tempfile `location'_`year'_`sex'
			save ``location'_`year'_`sex'', replace
			restore

			preserve
			// get draws from most recent best DisMod model (location/year/sex specific) for excess mortality
			keep if year_id == `year' & sex_id == `sex' & measure_id == 9
			drop measure_id
			merge 1:1 age_group_id using ``location'_`year'_`sex'', nogen // merging excess mortality files with remission draws

			// calculate duration
			forvalues c = 0/999 {
				qui replace rem_`c' = 1 / (draw_`c' + rem_`c')
				qui rename rem_`c' dur_`c'
			}

			// calculate survival rate
			forvalues c = 0/999 {
				qui replace draw_`c' = exp(-draw_`c' * dur_`c') // v_`c' = 1 - cfr --> survival rate
				qui rename draw_`c' v_`c'
				drop dur_`c'
			}

			order modelable_entity_id location_id year_id age_group_id sex_id v_*
			save "FILEPATH", replace
			restore
		}
	}

// write check here
	file open finished using "FILEPATH", replace write
	file close finished

// close logs
	if `close' log close
	clear
