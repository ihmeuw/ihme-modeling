// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Description:	Scale to population at risk where necessary

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

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

	// base directory on J
	local root_j_dir `1'
	// base directory on clustertmp
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
	local in_dir "FILEPATH"
	// directory for output on the J drive
	local out_dir "FILEPATH"
	// directory for output on clustertmp
	local tmp_dir "FILEPATH"
	// directory for standard code file
	adopath + FILEPATH
	adopath + FILEPATH
	// shell file
	local shell_file "FILEPATH/stata_shell.sh"

	// get demographics
    get_location_metadata, location_set_id(35) clear
    keep if most_detailed == 1 & is_estimate == 1

    // set locations for parallelization
    // must use location_id here because ihme_loc_id don't exist for all english subnationals
    levelsof location_id, local(locations)
    keep ihme_loc_id location_id location_name region_name
    tempfile location_data
    save `location_data'
    clear

    // get demographics
   	/*get_demographics, gbd_team(ADDRESS) clear
	local years = "$year_ids"
	local sexes = "$sex_ids" */
	local years 1990 1995 2000 2005 2010 2017
	local sexes 1 2

	// write log if running in parallel and log is not already open
	cap log using "FILEPATH/`step_num'.smcl", replace
	if !_rc local close_log 1
	else local close_log 0

	// check for finished.txt produced by previous step
	cap erase "`out_dir'/finished.txt"
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir "FILEPATH" dirs "`step'_*", respectcase
			// remove extra quotation marks
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file "FILEPATH/finished.txt"
			if _rc {
				di "`dir' failed"
				BREAK
			}
		}
	}

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

	// make draws folder
	! mkdir "`tmp_dir'/draws"

// parallelize code to pull in unadjusted dismod outputs and split into species specific prevalences
	local a = 0
	// erase and make directory for finished checks
	! mkdir "FILEPATH"
	local datafiles: dir "FILEPATH" files "finished_loc*.txt"
	foreach datafile of local datafiles {
		rm "FILEPATH"
	}

	**local locations 491 494 496 497 503 504 506 507 514 516 520 521

	foreach location of local locations {
		capture confirm file "FILEPATH/`location'.csv"
		if _rc {
		// submit job
		local job_name "loc`location'_`step_num'"
		di "submitting `job_name'"
		// di in red "location is `location'" // for tests
		local slots = 4
		local mem = `slots' * 2

		! qsub -P proj_custom_models -N "`job_name'" -pe multi_slot `slots' -l mem_free=`mem' "`shell_file'" "`code_dir'/`step_num'_parallel_test3.do" ///
		"`date' `step_num' `step_name' `location' `code_dir' `in_dir' `out_dir' `tmp_dir' `root_tmp_dir' `root_j_dir'"

		local ++ a
		sleep 100
		}

	}


	sleep 120000

// wait for jobs to finish ebefore passing execution back to main step file
	local b = 0
	while `b' == 0 {
		local checks : dir "FILEPATH" files "finished_loc*.txt", respectcase
		local count : word count `checks'
		di "checking `c(current_time)': `count' of `a' jobs finished"
		if (`count' == `a') continue, break
		else sleep 6000
	}

	// run save_results
	**qui run "FILEPATH/save_results.do"

	save_results_epi, modelable_entity_id(2797) mark_best(TRUE) db_env("prod") input_file_pattern({location_id}.csv) description("All-species PAR adjustment `date', old dismod model") input_dir(`tmp_dir'/draws) measure_id(5) clear

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)

	// write check file to indicate step has finished
		file open finished using "`out_dir'/finished.txt", replace write
		file close finished

	// if step is last step, write finished.txt file
		local i_last_step 0
		foreach i of local last_steps {
			if "`i'" == "`this_step'" local i_last_step 1
		}

		// only write this file if this is one of the last steps
		if `i_last_step' {

			// account for the fact that last steps may be parallel and don't want to write file before all steps are done
			local num_last_steps = wordcount("`last_steps'")

			// if only one last step
			local write_file 1

			// if parallel last steps
			if `num_last_steps' > 1 {
				foreach i of local last_steps {
					local dir: dir "FILEPATH" dirs "`i'_*", respectcase
					local dir = subinstr(`"`dir'"',`"""',"",.)
					cap confirm file "FILEPATH/finished.txt"
					if _rc local write_file 0
				}
			}

			// write file if all steps finished
			if `write_file' {
				file open all_finished using "FILEPATH/finished.txt", replace write
				file close all_finished
			}
		}

	// close log if open
		if `close_log' log close
