// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		Master script to submit the long-term matrices and N-code prevalence for COMO input

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
		local 4 "05a"
		local 5 scaled_short_term_en_prev_yld_by_platform

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
	// directory for output on the FILEPATH drive
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on FILEPATH
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	// directory for standard code files
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
local outputs N E NE_matrix

// If no check global passed from master, assume not a test run
	if missing("$check") global check 0

// Filepaths
	local diag_dir "`tmp_dir'/FILEPATH"
	local draw_dir "`tmp_dir'/FILEPATH"
	local summ_dir "`tmp_dir'/FILEPATH"
	local rundir "`root_tmp_dir'/FILEPATH/`date'"
	local stepfile "`code_dir'/FILEPATH.xlsx"
	
// Import functions
	adopath + "`code_dir'/ado"
	
	start_timer, dir("`diag_dir'") name("`step_name'") slots(1)
	
// Make necessary output directories
foreach output of local outputs {
	foreach loc in draw_dir diag_dir {
		cap mkdir "`loc'/`output'"
	}
}

// SET LOCALS
// set memory (gb) for each job
	local mem 2
	local type "epi"
	local subnational "yes"
	local code "`step_name'/FILEPATH.do"

get_demographics, gbd_team(epi) clear
global year_ids `r(year_ids)'
global location_ids `r(location_ids)'
global sex_ids `r(sex_ids)'

if `tester' == 1 {
	global location_ids 568
	global year_ids 2010
	global sex_ids 1
}

// parallelize by location/year/sex
foreach location_id of global location_ids {
	foreach year of global year_ids {
		foreach sex of global sex_ids {
		! qsub -P proj_injuries_2 -N _`step_num'_`location_id'_`year'_`sex' -pe multi_slot 4 -l mem_free=8 "`code_dir'/FILEPATH.sh" "`code_dir'/`code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `year' `sex'"
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
	file open finished using "`tmp_dir'/finished.txt", replace write
	file close finished
	