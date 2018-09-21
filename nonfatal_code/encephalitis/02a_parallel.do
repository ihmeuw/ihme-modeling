// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		
// Last updated:	
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
	adopath + "SHARED FUNCTIONS"
	// functional
	local functional "encephalitis"

	// get locals from demographics
	get_demographics, gbd_team(epi) clear
	local years = r(year_ids)
	local sexes = r(sex_ids)
	local age_group_ids = r(age_group_ids)

	// test run
	//local years 2000 2005
	//local sexes 1 

// write log if running in parallel and log is not already open
	cap log using "`out_dir'/02_temp/02_logs/`step_num'_`location'.smcl", replace
	if !_rc local close 1
	else local close 0

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE

// get draws from most recent best DisMod model for location (remission (7) and excess mortality (9))
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(1419) measure_ids(7 9) location_ids(`location') source(epi) status(best) age_group_ids(`age_group_ids') clear
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
			save "`tmp_dir'/03_outputs/01_draws/dm-`functional'-survive-`location'_`year'_`sex'.dta", replace
			restore
		}
	}

// write check here
	file open finished using "`tmp_dir'/02_temp/01_code/checks/finished_loc`location'.txt", replace write
	file close finished

// close logs
	if `close' log close
	clear
