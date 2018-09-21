// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author:		USERNAME
// Description:	Use SMR data with envelope  to generate excess mortality 

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
// Global can't be passed from master when called in parallel
	if "`1'" == "" {
		local 1 FILEPATH
		local 2 FILEPATH
		local 3 DATE
		local 4 "05b"
		local 5 long_term_inc_to_raw_prev

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
	cap log using "`out_dir'/FILEPATH.smcl", replace name(step_parent)
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

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE

local gen_excessmort 0

// Settings

	** are you debugging?
	local debug 0
	local tester 0
	
	set type double, perm
	set seed 0
	if missing("$check") global check 0
	
// Filepaths
	local diag_dir "`out_dir'/FILEPATH"
	local stepfile "`code_dir'/FILEPATH.xlsx"
	local smr_data "`in_dir'/FILEPATH.csv"
	local gbd_ado "FILEPATH"
	
// Bring in other ado files
	adopath + `gbd_ado'
	adopath + `code_dir'/ado
	
// Start timer
	local slots 4
	start_timer, dir("`diag_dir'") name("`step_name'") slots(`slots')
	
// Load parameters
	load_params
	get_demographics, gbd_team(epi) clear
	return list
	global year_ids `r(year_ids)'
	global location_ids `r(location_ids)'
	global sex_ids `r(sex_ids)'

local code "`step_name'/FILEPATH.do"

if `tester' == 1 {
	global location_ids 44550
}

		foreach location_id of global location_ids {
			foreach year_id of global year_ids {
				foreach sex_id of global sex_ids {
							! qsub -o FILEPATH -P proj_injuries -N _`step_num'_`location_id'_`year_id'_`sex_id' -pe multi_slot 4 -l mem_free=8 -p -2 "`code_dir'/FILEPATH.sh" "`code_dir'/`code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `year_id' `sex_id'"
							local n = `n' + 1
				}
			}
		}		

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

