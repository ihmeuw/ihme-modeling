// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
//				Globals will be carried over from 00_master.do
// Author:		USERNAME
// Description:	Submit jobs for shock mortality step
** *********************************************
// DON'T EDIT - prep stata
** *********************************************

// PREP STATA (DON'T EDIT)
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
		global prefix "FILEPATH:"
	}

// Check for parallelizing 
global parallel_check = 0

// Global can't be passed from master when called in parallel
	if "`1'" == "" {
		local 1 FILEPATH
		local 2 FILEPATH
		local 3 DATE
		local 4 "03a"
		local 5 impute_short_term_shock_inc

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
	// directory for output on the J drive
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on clustertmp
	local tmp_dir "`root_tmp_dir'/FILEPATH"

	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/FILEPATH.smcl", replace
	if !_rc local close_log 1
	else local close_log 0

	// check for finished.txt produced by previous step
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir "`root_j_dir'/FILEPATH" dirs "`step'_*", respectcase
			// remove extra quotation marks
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file "`root_j_dir'/FILEPATH.txt"
			if _rc {
				di "`dir' failed"
				BREAK
			}
		}
	}	

** *********************************************
** WRITE CODE HERE
** *********************************************

// SETTINGS
	** debugging?
	local debug 0
	** how many slots is this script being run on?
	local slots 4
	
// Filepaths
	local gbd_ado "FILEPATH"
	local diag_dir "FILEPATH"
	local stepfile "FILEPATH"
	local rundir "FILEPATH"
	
// Import functions
	adopath + "`code_dir'/ado"
	adopath + `gbd_ado'
	start_timer, dir("`diag_dir'") name("`step_name'") slots(`slots')
	
// set memory (gb) for each job
	local mem 8
	local type "epi"
	local subnational "yes"

// parallelize by location/sex
if `mem' < 2 local mem 2
local slots = ceil(`mem'/2)
local mem = `slots' * 2

// submit jobs
! rm -rf "`tmp_dir'/FILEPATH"
! mkdir "`tmp_dir'/FILEPATH"

get_demographics , gbd_team(epi) clear

global location_ids `r(location_ids)'
global sex_ids `r(sex_ids)'

local code "FILEPATH.do" 
** submit jobs from here
foreach location_id of global location_ids {
		foreach sex_id of global sex_ids {
			! qsub -P proj_injuries_2 -N _`step_num'_`location_id'_`sex_id' -pe multi_slot 2 "`code_dir'/FILEPATH.sh" "`code_dir'/FILEPATH.do" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `sex_id'"
		}
	}



// write check file to indicate step has finished - this is now happening in the jobs that are being submitted above
	file open finished using "`out_dir'/finished.txt", replace write
	file close finished
// End timer and sum up substep times
	end_timer, dir("`diag_dir'") name("`step_name'")
	//sum_times, dir("`diag_dir'")

// Close log if open
	log close worker
