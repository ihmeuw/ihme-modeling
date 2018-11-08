// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		
// Last updated:	4/27/16
// Description:	Parallelization of 09_collate_outputs_new

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
	local code_dir 		`4'
	local in_dir 		`5'
	local out_dir 		`6'
	local tmp_dir 		`7'
	local root_tmp_dir 	`8'
	local root_j_dir 	`9'
	local cause 		`10'
	local group			`11'
	local state 		`12'
	local id 			`13'

	// metric -- measure_id for prevalence (5), save_results uses measure, not measure_id
	local metric 5

	// directory for standard code files
	adopath + "FILEPATH"
	
	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/02_temp/02_logs/`step_num'_`cause'_`group'_`state'_`id'.smcl", replace
	if !_rc local close 1
	else local close 0
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE

	run "FILEPATH"
	
	save_results_epi, input_dir("FILEPATH") input_file_pattern("{measure_id}_{location_id}_{year_id}_{sex_id}.csv") modelable_entity_id(`id') ///
	description("Custom `functional' `date' - custom code results") measure_id("5") sex_id("1 2") mark_best(True) clear
	
	// write check here
	file open finished using "FILEPATH", replace write
	file close finished

	if `close' log close
	clear
