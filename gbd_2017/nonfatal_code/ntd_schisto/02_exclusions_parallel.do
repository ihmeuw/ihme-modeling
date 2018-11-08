// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Last updated:	01/18/2017
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

/*	local date 			2017_01_23
	local step_num 		02
	local step_name		"sequela_split"
	local location 		491
	local code_dir 		"FILEPATH"
	local in_dir 		"FILEPATH"
	local out_dir 		"FILEPATH"
	local tmp_dir 		"FILEPATH"
	local root_tmp_dir 	"FILEPATH"
	local root_j_dir 	"FILEPATH"
*/
	// write log if running in parallel and log is not already open
	cap log using "FILEPATH/exclusion_`location'.smcl", replace
	if !_rc local close 1
	else local close 0

	// directory for standard code file
	adopath + FILEPATH

	** demographics
	local years 1990 1995 2000 2005 2010 2017
	local sexes 1 2

	** set meids
	local meids 1468 1469 1470 1471 1472 1473 1474 1475

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

	** pull in draws
	get_draws, gbd_id_type(modelable_entity_id) gbd_id(2797) source(ADDRESS) measure_id(5) location_id(`location') status(best) clear
	drop if age_group_id == 22 | age_group_id == 27
	keep measure_id location_id year_id age_group_id sex_id draw_*

	** these should all be prevalence, so nothing is done
	foreach meid of local meids {
		export delimited "FILEPATH/`location'.csv", replace
	}

	** write check here
	file open finished using "FILEPATH/finished_loc`location'.txt", replace write
	file close finished

	** close logs
	if `close' log close
	clear
