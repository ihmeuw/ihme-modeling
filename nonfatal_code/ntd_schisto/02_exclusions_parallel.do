// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		USERNAME
// Last updated:	DATE
// Description:	Parallelization of 02_sequela_split for excluded areas

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
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

	// write log if running in parallel and log is not already open
	cap log using "FILEPATH/exclusion_`location'.smcl", replace
	if !_rc local close 1
	else local close 0

	// directory for standard code file
	adopath + FILEPATH

	** demographics
	local years 1990 1995 2000 2005 2010 2016
	local sexes 1 2

	** set meids
	local meids 1468 1469 1470 1471 1472 1473 1474 1475

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
** 

	** pull in draws
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(2797) source(epi) measure_ids(5) location_ids(`location') status(best) clear
	drop if age_group_id == 22 | age_group_id == 27
	keep measure_id location_id year_id age_group_id sex_id draw_*

	** these should all be prevalence
	foreach meid of local meids {
		export delimited "FILEPATH/`location'.csv", replace
	}
	
** finish up	
	** write check here
	file open finished using "FILEPATH/finished_loc`location'.txt", replace write
	file close finished

	** close logs
	if `close' log close
	clear

	