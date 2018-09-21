// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author:		USERNAME
// Description:	apply probability of long-term outcomes to short-term incidence to get incidence of long-term outcomes by ecode-ncode-platform
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	set type double, perm
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

		local 8 "/FILEPATH"
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

// Filepaths
// Filepath to where pyHME function library is located
	local func_dir "`code_dir'"
	local diag_dir "FILEPATH"
	local pops_dir "`tmp_dir'/FILEPATH"
	local rundir "`root_tmp_dir'/FILEPATH/`FILEPATH'"
	local checkfile_dir "`tmp_dir'/FILEPATH"
	local ode_checkfile_dir "`tmp_dir'/FILEPATH"
	local prev_results_dir "`tmp_dir'/FILEPATH"
	
// Import functions
	adopath + "`code_dir'/ado"
	adopath + "`func_dir'"
	adopath + "FILEPATH"
	
	local slots 1
	start_timer, dir("`diag_dir'") name("`step_name'") slots(`slots')

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

	// are you just testing this code and don't want to erase/zip any previous files at the end?
	local debug 1
	local tester 1

// Settings
	** two ways of sending excess mortality data to dismod "SMR" or "chi"; try both ways
	global SMR_or_chi "chi"
	** how much memore does the prep_ltinc step need
	local prep_ltinc_mem 2
	** how much memory does the prep_ltinc step need for shocks
	local prep_ltinc_mem_shocks 4
	** how much memory does the data_in/rate_in generation script need
	local data_rate_mem 4
	** how much memory does creating the value_in files need
	local value_in_mem 2
	** how much memory does appending the results take
	local append_results_mem 2
	
// start_timer
	local diag_dir "`tmp_dir'/FILEPATH"
	start_timer, dir("`diag_dir'") name("`step_name'")
	
// Load parameters
	load_params

	local platforms inp otp

	get_demographics, gbd_team(epi) clear
		global year_ids `r(year_ids)'
		global location_ids `r(location_ids)'
		global sex_ids `r(sex_ids)'
		global age_group_ids `r(age_group_ids)'

// Step 2 prep long-term incidence files in the right format
	if $prep_ltinc {

		local prep_ltinc_mem 2
		if $nonshock {
			local code "`step_name'/FILEPATH.do"
			foreach location_id of global location_ids {
				foreach year_id of global year_ids {
					foreach sex_id of global sex_ids {
						! qsub -P proj_injuries -N _`step_num'_`location_id'_`year_id'_`sex_id' -pe multi_slot 4 -l mem_free=8 -p -2 "`code_dir'/FILEPATH.sh" "`code_dir'/`code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `year_id' `sex_id'"
						}
					}
				}
			}

		if $shock {
			local shock_code "`step_name'/FILEPATH.do"
			import excel using "`code_dir'/_inj_steps.xlsx", firstrow clear
			foreach location_id of global location_ids {
				foreach platform of local platforms {
					foreach sex_id of global sex_ids {
						! qsub -P proj_injuries -N _`step_num'_`location_id'_`platform'_`sex_id' -pe multi_slot 4 -l mem_free=8 -p -2 "`code_dir'/FILEPATH.sh" "`code_dir'/`shock_code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `sex_id' `platform'"
						}
					}
				}		
			di "all shocks submitted with slots: `slots', mem: `mem'."
		}
		
	}


