// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code specifically for save_results
// Author:		
// Last updated:	1/11/2016
// Description:	Save results parallelization of 04a_dismod_prep_wmort

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

// do "FILEPATH"

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
	local cause			`10'
	local group 		`11'
	local state 		`12'
	
	// directory for standard code files	
	adopath + "FILEPATH"
	// functional
	local functional "meningitis"
	
	local file_pat "{location}.csv"

	// write log if running in parallel and log is not already open
	cap log using "FILEPATH", replace
	if !_rc local close 1
	else local close 0

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE

	use "FILEPATH", clear
	keep if healthstate == "epilepsy_any" | healthstate == "_unsqueezed" | healthstate == "_parent"
	keep if grouping == "_epilepsy" | grouping == "_vision" | grouping == "_hearing" | grouping == "long_modsev"
	

	levelsof modelable_entity_id if acause == "`cause'" & grouping == "`group'" & healthstate == "`state'", local(ids)
	
	run "FILEPATH"	
	foreach id of local ids {
		
		if "`group'" == "_hearing" | "`group'" == "_vision" {
			
			save_results_epi, input_dir("FILEPATH") input_file_pattern("{measure_id}_{location_id}_{year_id}_{sex_id}.csv") modelable_entity_id(`id') description("Custom `functional' `date' unsqueezed - custom code results") measure_id("5") sex_id("1 2") mark_best(True) clear

		}
		
		if "`group'" == "_epilepsy" {
			
			save_results_epi, input_dir("FILEPATH") input_file_pattern(`file_pat') modelable_entity_id(`id') description("Custom `functional' `date' `cause' ODE epilepsy results") measure_id("5") sex_id("1 2") mark_best(True) clear
			
		}

		if "`group'" == "long_modsev" {

			save_results_epi, input_dir("FILEPATH") input_file_pattern(`file_pat') modelable_entity_id(`id') description("Custom `functional' `date' `cause' ODE epilepsy results") measure_id("5") sex_id("1 2") mark_best(True) clear
		
		} 
	
		local id `id'

		// write check here
		file open finished using "FILEPATH", replace write
		file close finished
	}

	// close logs
	if `close' log close
	clear
