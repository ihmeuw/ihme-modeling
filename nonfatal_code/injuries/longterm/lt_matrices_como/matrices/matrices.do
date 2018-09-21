// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		child script for long-term matrices and n-codes
// Description:	This step cycles through n-codes, sums long term prevalence across e-codes for input into COMO and also saves the distributions of E-codes for each n-code for post-como redistribution
// Calls a function in the /ado folder
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

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
	if "`1'"=="" {
		local 1 FILEPATH
		local 2 FILEPATH
		local 3 DATE
		local 4 "07"
		local 5 long_term_final_prev_and_matrices
		local 6 "FILEPATH"
		local 7 160
		local 8 2016
		local 9 1
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
    // directory where the code lives
    local code_dir `6'
    // iso3
	local location_id `7'
	// year
	local year `8'
	// sex
	local sex `9'
	// directory for external FILEPATH
	local in_dir "`root_j_dir'/FILEPATH"
	// directory for output on the FILEPATH drive
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on FILEPATH
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	// directory for standard code files
	adopath + "FILEPATH"
	
	// write log if running in parallel and log is not already open
	local log_file "`tmp_dir'/FILEPATH.smcl"
	log using "`log_file'", replace name(worker)
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// SETTINGS
	set type double, perm
	** how many slots is this script being run on?
	local slots 4
	** debugging?
	local debug 0

// Filepaths
	local como_dir = "FILEPATH"
	cap mkdir "`como_dir'"
	local diag_dir "`tmp_dir'/FILEPATH"
	local rundir "`root_tmp_dir'/FILEPATH/`date'"
	local draw_dir "`tmp_dir'/FILEPATH"
	local summ_dir "`tmp_dir'/FILEPATH"
	local ncode_draw_out "`como_dir'/FILEPATH"
	local NEmatrix_draw_out "`como_dir'/FILEPATH"
	local NEmatrix_summ_out "`summ_dir'/FILEPATH"
	cap mkdir "`ncode_draw_out'"
	cap mkdir "`NEmatrix_draw_out'"
	cap mkdir "`NEmatrix_summ_out'"
	cap mkdir "`summ_dir'"
	cap mkdir "`draw_dir'"

// Import functions
	adopath + "`code_dir'/ado"

// Start timing
	start_timer, dir("`diag_dir'") name("`location_id'_`year'_`sex'") slots(`slots')

// Load injuries parameters
	get_demographics, gbd_team(epi) clear
	global year_ids `r(year_ids)'
	global age_group_ids `r(age_group_ids)'
	global sex_ids `r(sex_ids)'
	global location_ids `r(location_ids)'

// get the step number of the step you're pulling output from
	local step_name long_term_final_prev_by_platform
	import excel "`code_dir'/_inj_steps.xlsx", sheet("steps") firstrow clear
	keep if name == "`step_name'"
	local prev_step = step[1] + "_`step_name'"

// Import long-term prev by E-N
	use "`rundir'/`prev_step'/FILEPATH.dta", clear
	cap drop mean
	cap drop prob_draw_

	drop if regexm(ncode, "N") != 1
	
	// Get age groups (not ids)
	preserve
	insheet using "`code_dir'/FILEPATH.csv", comma names clear
	levelsof age_start, l(ages)
	restore

// Create prevalence NE matrices and collapse to N and E-code long-term prevalences
	collapse_to_n_and_e, code_dir("`code_dir'") prefix("$prefix") n_draw_outdir("`ncode_draw_out'") e_draw_outdir("`draw_dir'") summ_outdir("`summ_dir'") output_name("5_`location_id'_`year'_`sex'") ages("`ages'") longterm matrix_draw_outfile("FILEPATH.csv") matrix_summ_outfile("FILEPATH.csv")
	
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

	
