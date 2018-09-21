// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:		[enter your name here]
// Description:	[enter description of this step]

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}
	if "`1'" == "" {
		local 1 FILEPATH
		local 2 FILEPATH
		local 3 DATE
		local 4 "06b"
		local 5 long_term_final_prev_by_platform

		local 8 "FILEPATH"
	}
	// base directory on FILEPATH 
	local root_j_dir `1'
	// base directory on FILEPATH
	local root_tmp_dir `2'
	// timestamp of current run (i.e. 2014_01_17)
	local date `3'
	// step number of this step (i.e. 01a)
	local step_num `4'
	// name of current step (i.e. first_step_name)
	local step_name `5'
	// step numbers of immediately anterior parent step (i.e. for step 2: 01a 01b 01c)
	local hold_steps `6'
	// step numbers for final steps that you are running in the current run (i.e. 11a 11b 11c)
	local last_steps `7'
    // directory where the code lives
    local code_dir `8'
	// directory for external inputs
	local in_dir "`root_j_dir'/FILEPATH"
	// directory for output on FILEPATH
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on FILEPATH
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	// directory for standard code files
	adopath + "FILEPATH"
	adopath + "FILEPATH"

	// write log if running in parallel and log is not already open
	log using "`tmp_dir'/FILEPATH.smcl", replace name(master)

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// Settings
	local debug 1
	local tester 1
	set type double, perm

	local annual 1

// If no check global passed from master, assume not a test run
	if missing("$check") global check 0
	
// Filepaths
	local diag_dir "`tmp_dir'/FILEPATH"
	local summ_dir "`tmp_dir'/FILEPATH"
	local rundir "`code_dir'/FILEPATH"
	local stepfile "`code_dir'/_inj_steps.xlsx"
	
	cap mkdir "`out_dir'"
	cap mkdir "`out_dir'/FILEPATH"
	cap mkdir "`diag_dir'"
	cap mkdir "`tmp_dir'"
	cap mkdir "`summ_dir'"
	
// Import functions
	adopath + "`code_dir'/ado"
	
	start_timer, dir("`diag_dir'") name("`step_name'") slots(1)

// set memory (gb) for each job
	local mem 2
// set type for pulling different years (cod/epi); this is used for what parellel jobs to submit based on cod/epi estimation demographics, not necessarily what inputs/outputs you use
	local type "epi"
// set subnational=no (drops subnationals) or subnational=yes (drops national where subnationals exist)
	local subnational "yes"
// set code file from 01_code to run in parallel
	local code "`step_name'/FILEPATH.do"
		
// parallelize by location/year/sex
if `annual' == 0 {

	get_demographics, gbd_team(epi) clear
	global location_ids `r(location_ids)'
	global sex_ids `r(sex_ids)'
	global age_group_ids `r(age_group_ids)'
	global year_ids `r(year_ids)'

	if `tester' == 1 {
		global location_ids 44718
		global year_ids 2010
		global sex_ids 1
	}

	foreach location_id of global location_ids {
		foreach year of global year_ids {
			foreach sex of global sex_ids {
				! qsub -e FILEPATH -o FILEPATH -P proj_injuries -N _`step_num'_`location_id'_`year'_`sex' -pe multi_slot 4 -l mem_free=8 "`code_dir'/FILEPATH.sh" "`code_dir'/`code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `year' `sex'"
			}
		}
	}
}

if `annual' == 1 {
	local code "`step_name'/FILEPATH.do"
		
// parallelize by location/year/sex
	get_demographics, gbd_team(epi) clear
	global location_ids `r(location_ids)'
	global sex_ids `r(sex_ids)'
	global age_group_ids `r(age_group_ids)'

	if `tester' == 1 {
		global location_ids 35643
		global sex_ids 1
		global year_ids 1990
	}

	* 35628 1993 1

	foreach location_id of global location_ids {
		foreach year of global year_ids {
			foreach sex of global sex_ids {
				! qsub -e FILEPATH -o FILEPATH -P proj_injuries -N _`step_num'_annual_`location_id'_`year'_`sex' -pe multi_slot 4 -l mem_free=8 "`code_dir'/FILEPATH.sh" "`code_dir'/`code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `year' `sex'"
			}
		}
	}
}
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)

// End timer
	end_timer, dir("`diag_dir'") name("`step_name'")
	//sum_times, dir("`diag_dir'")
	
// close log if open
	log close master

// write check file to indicate step has finished
	file open finished using "`tmp_dir'/FILEPATH.txt", replace write
	file close finished
	