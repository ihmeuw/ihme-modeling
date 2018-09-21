// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		
// Last updated:	
// Description:	Parallelization of 09_collate_outputs

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set mem 2g
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
	local code_dir 		`4'
	local in_dir 		`5'
	local out_dir 		`6'
	local tmp_dir 		`7'
	local root_tmp_dir 	`8'
	local root_j_dir 	`9'
	local cause 		`10'
	local group			`11'
	local state 		`12'

	// functional
	local functional "encephalitis"
	// metric -- measure_id for prevalence(5)
	local metric 5
	// directory for standard code files
	adopath + "SHARED FUNCTIONS"

	// get demographics and locations
	get_location_metadata, location_set_id(9) clear
	keep if most_detailed == 1 & is_estimate == 1
	levelsof location_id, local(locations)
	clear
	get_demographics, gbd_team(epi) clear
	local years = r(year_ids)
	local sexes = r(sex_ids)
	clear

	// directory for pulling files from previous step
	local pull_dir_05b "`root_tmp_dir'/03_steps/`date'/05b_sequela_split_woseiz/03_outputs/01_draws"

	
	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/02_temp/02_logs/`step_num'_`cause'_`group'_`state'.smcl", replace
	if !_rc local close 1
	else local close 0
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
	use "`in_dir'/encephalitis_dimension.dta", clear
	drop if healthstate == "_parent" | grouping == "_epilepsy" | grouping == "cases" | grouping == "_vision"
	gen num = ""

	replace num = "05b" if grouping == "long_mild" | grouping == "long_modsev"
	gen name = ""
	replace name = "outcome_prev_womort" if grouping == "long_mild" | grouping == "long_modsev"

	preserve

	levelsof num if acause == "`cause'" & grouping == "`group'" & healthstate == "`state'", local(num) clean
	local n `num' // num is always 05b

	/*
	//test run 
	local locations 69 207
	local years 2000 2005
	local sexes 1
	*/
	foreach location of local locations {
		foreach year of local years {
			foreach sex of local sexes {
				use "`pull_dir_`n''/`cause'_`state'_`location'_`year'_`sex'.dta", clear
				if "`group'" == "long_mild"{
				drop measure_id modelable_entity_id grouping
				}
				if "`group'" == "long_modsev"{
				drop measure_id modelable_entity_id
				}
				outsheet using "`tmp_dir'/03_outputs/01_draws/`cause'/`group'/`state'/`metric'_`location'_`year'_`sex'.csv", comma replace // might need to change this for viral
				clear
			}
		}
	}

	// write check here
	file open finished using "`tmp_dir'/02_temp/01_code/checks/finished_`cause'_`group'_`state'.txt", replace write
	file close finished

	if `close' log close
	clear
