// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		
// Last updated:	
// Description:	Parallelization of 04d_outcome_prev_wmort_woseiz.do

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

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
	// etiologies
	local etiologies "meningitis_pneumo meningitis_hib meningitis_meningo meningitis_other"
	// grouping
	local grouping "long_modsev"

	// get locals from demographics
	get_demographics, gbd_team(epi) clear
	local years = r(year_ids)
	local sexes = r(sex_ids)
	local age_group_ids = r(age_group_ids)

	//test run
	//local years 2000 2005
	//local sexes 1

	// sequelae meids
	local meningitis_pneumo_long_modsev = 1305
	local meningitis_hib_long_modsev = 1335
	local meningitis_meningo_long_modsev = 1365
	local meningitis_other_long_modsev = 1395

	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/02_temp/02_logs/`step_num'_`location'.smcl", replace
	if !_rc local close 1
	else local close 0

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
	foreach etiology of local etiologies {
		foreach group of local grouping {
		// pull prevalence DisMod for each long_modsev outcome
		get_draws, gbd_id_field(modelable_entity_id) gbd_id(``etiology'_`group'') location_ids(`location') measure_ids(5) status(best) source(epi) age_group_ids(`age_group_ids') clear
		drop model_version_id	
			foreach year of local years {
				foreach sex of local sexes {
					preserve
					keep if year_id == `year' & sex_id == `sex'
					save "`tmp_dir'/03_outputs/01_draws/`etiology'_`group'_`location'_`year'_`sex'.dta", replace
					restore
				}
			}
		}
	}

// write check here
	file open finished using "`tmp_dir'/02_temp/01_code/checks/finished_loc`location'.txt", replace write
	file close finished

// close logs
	if `close' log close
	clear
