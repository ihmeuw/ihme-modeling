// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		
// Last updated:	
// To do: Check file paths and directories
// Description:	Parallelization of 08_viral_prev

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
	local functional "meningitis"
	// grouping
	local grouping "viral"
	// healthstate
	local healthstate "inf_sev"
	// etiology
	local etiology "meningitis_other"

	// get demographics
	get_demographics, gbd_team(epi) clear
	local years = r(year_ids)
	local sexes = r(sex_ids)
	local age_group_ids = r(age_group_ids)
	clear

	//local years 2000 2005
	//local sexes 1 

	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/02_temp/02_logs/`step_num'_`location'.smcl", replace
	if !_rc local close 1
	else local close 0
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
	// get draws
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(1296) measure_ids(5) location_ids(`location') status(best) source(epi) age_group_ids(`age_group_ids') clear

	foreach year of local years {
		foreach sex of local sexes {
			preserve
			keep if year_id == `year' & sex_id == `sex'
			merge m:1 sex_id age_group_id using "`in_dir'/bac_vir_ratio_2016.dta", keep(3) nogen
			forvalues a = 0/999 {
				qui replace draw_`a' = draw_`a' * ratio
			}
			drop ratio
			sort age_group_id
			cap mkdir "`tmp_dir'/03_outputs/01_draws/`etiology'"
			save "`tmp_dir'/03_outputs/01_draws/`etiology'/meningitis_other_`healthstate'_`location'_`year'_`sex'.dta", replace // this format is for save results later
			restore
		}
	}

	
// write check here
	file open finished using "`tmp_dir'/02_temp/01_code/checks/finished_loc`location'.txt", replace write
	file close finished

// close logs
if `close' log close
clear
