// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		
// Last updated:	04/22/16
// To do: Check file paths and directories
// Description:	Parallelization of 08_viral_prev; need to update bac_vir_ratio per year once hospital data is out 

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
	adopath + "FILEPATH"
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
	local years = r(year_id)
	local sexes = r(sex_id)
	local age_group_ids = r(age_group_id)
	

	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/02_temp/02_logs/`step_num'_`location'.smcl", replace
	if !_rc local close 1
	else local close 0
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
	// get draws
	run "FILEPATH"
	get_draws, gbd_id_type(modelable_entity_id) gbd_id(1296) measure_id(5 6) location_id(`location') status(best) source(epi) age_group_id(`age_group_ids') clear

	foreach year of local years {
		foreach sex of local sexes {
			preserve
			keep if year_id == `year' & sex_id == `sex'
			//need to update the flat file for the new age groups
			merge m:1 sex_id age_group_id using "FILEPATH", keep(3) nogen
			forvalues a = 0/999 {
				qui replace draw_`a' = draw_`a' * ratio
			}
			drop ratio
			sort age_group_id
			cap mkdir "FILEPATH"
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
