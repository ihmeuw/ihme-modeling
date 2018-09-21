// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:		 USERNAME
// Description: 	submit to the cluster files that will take the Dismod output for all of the e-code models and split the results into inpatient/not-inpatient
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
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
	if missing("$check") global check 0

	local pull_covs = 0

	if "`1'" == "" {
		local 1 FILEPATH
		local 2 FILEPATH
		local 3 DATE
		local 4 "02a"
		local 5 raw_nonshock_short_term_ecode_inc_by_platform

		local 8 "FILEPATH"
	}
	// base directory on FILEPATH
	local root_j_dir `1'
	di "1: root_j_dir: `root_j_dir'"
	// base directory on FILEPATH
	local root_tmp_dir `2'
	di "2: root_tmp_dir: `root_tmp_dir'"
	// timestamp of current run (i.e. 2014_01_17)
	local date `3'
	di "3: date: `date'"
	// step number of this step (i.e. 01a)
	local step_num `4'
	di "4: step_num: `step_num'"
	// name of current step (i.e. first_step_name)
	local step_name `5'
	di "5: step_name: `step_name'"
	// step numbers of immediately anterior parent step (i.e. for step 2: 01a 01b 01c)
	local hold_steps `6'
	di "6: hold_steps: `hold_steps'"
	// step numbers for final steps that you are running in the current run (i.e. 11a 11b 11c)
	local last_steps `7'
	di "7: last_steps `last_steps'"
    // directory where the code lives
    local code_dir `8'
    di "8: code_dir `code_dir'"
	// directory for external inputs
	local in_dir "`root_j_dir'/FILEPATH"
	// directory for output on the J drive
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on clustertmp
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	
	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/FILEPATH.smcl", replace name(log_`step_num')
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
// WRITE CODE HERE
** *********************************************

// Settings
	local debug 0
	local testing 0

	local redo 1

// Filepaths
	local gbd_ado "FILEPATH"
	local diag_dir "`out_dir'/FILEPATH"
	
// Import GBD functions
	adopath + "`gbd_ado'"
	adopath + `code_dir'/ado
	
start_timer, dir("`diag_dir'") name("`step_name'")
	
// Load injury parameters
	load_params
	
// set memory (gb) for each job
	local mem 2
// set type for pulling different years (cod/epi); this is used for what parellel jobs to submit based on cod/epi estimation demographics, not necessarily what inputs/outputs you use
	local type "epi"
// set subnational=no (drops subnationals) or subnational=yes (drops national CHN/IND/MEX/GBR)
	local subnational "yes"
	local metric incidence
// set code file from 01_code to run in parallel (change from template; just "name.do" no file path since it should be in same directory)
	local code "`step_name'/FILEPATH.do" 

** Pull the list of e-codes that are modeled by DisMod that we are going to transform
	insheet using "`code_dir'/FILEPATH.csv", comma names clear
		keep if injury_metric == "Adjusted data"
		drop if e_code == "inj_war" | e_code == "inj_disaster"
		levelsof modelable_entity_id, l(me_ids) clean
		keep modelable_entity_id e_code
		tempfile mes 
		save `mes', replace

// SAVE ANY COMMON INPUTS NEEDED BY PARALLELIZED JOBS
		// best epi models
if `pull_covs' == 1 {
	foreach model of local me_ids {
		get_best_model_versions, entity(modelable_entity) ids(`model') status(best) clear
		tostring model_version_id, replace
		tostring modelable_entity_id, replace

		levelsof acause, l(acause) clean
		levelsof model_version_id, l(version) clean

		cap !gunzip FILEPATH.gz
		import delimited "FILEPATH.csv", delim(",") varn(1) clear

		keep beta_incidence_x_s_outpatient

		count
		local end_row = `r(N)'
		local start_row = `r(N)'-999
		keep in `start_row'/`end_row'
		gen inpatient = 1
		rename beta_incidence_x_s_outpatient outpatient
		replace outpatient = exp(outpatient)
		gen acause = "`acause'"
		gen draw = _n - 1
		reshape wide outpatient, i(inpatient acause) j(draw)
		cap mkdir "`out_dir'/FILEPATH"
		save "`out_dir'/FILEPATH.dta", replace
	}
}

		** end loop saving covariates

		get_demographics, gbd_team(epi) clear

		global year_ids `r(year_ids)'
		global location_ids `r(location_ids)'
		global sex_ids `r(sex_ids)'

		if `testing' == 1 {
			global year_ids 2016
			global location_ids 101 160
			global sex_ids 2
		}

		if `redo' {
			global year_ids 2016
			global sex_ids 2
			global location_ids  164   165   168   169   171   172   173   175   177   178   179   182   189   193 35619 35620 35621 35622 35623 35624 35625 35626 35627 35628 35629 35630 35631 35632 35634 35636 35637 35638 35639 35643 35646 35650 35655 35661 35662 35663 43873 43880 43882 43896 43903 43904 43906 43928 43929 43932 43937 43939 43941 44539 44540   482
		}

		foreach location_id of global location_ids {
			foreach year_id of global year_ids {
				foreach sex_id of global sex_ids {
					!qsub -e FILEPATH -o FILEPATH -P proj_injuries -N _`step_num'_`location_id'_`year_id'_`sex_id' -pe multi_slot 4 -l mem_free=8 -p -2 "`code_dir'/FILEPATH.sh" "FILEPATH.do" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `year_id' `sex_id'"
					}
				}
			}

** ***********************************************************
// Write check files
** ***********************************************************

	// write check file to indicate step has finished
		file open finished using "`out_dir'/FILEPATH.txt", replace write
		file close finished
	
	// End timer and aggregate substep times
		end_timer, dir("`diag_dir'") name("`step_name'")
		
	// close log if open
		log close log_`step_num'
		
