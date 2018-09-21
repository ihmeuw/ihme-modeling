// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:		
// Last updated:	
// Description:	Collate all meningitis-etiology-sequelae outputs in one place for transferring to COMO
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

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

	** ****************************************************************************
	** specify which sequelae to upload to epi_viz	(upload: 1; do not upload: 0)
		local upload_long_mild 		1	/* long term sequelae without mortality risk */
		local upload_long_modsev 	1	/* long term sequelae with mortality risk */
		local upload_cases 			1	/* short term sequela due to bacterial infection */
		local upload_viral 			1	/* short term sequela due to viral infection */
	** ****************************************************************************
	
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
	local in_dir "`root_j_dir'/02_inputs"
	// directory for output on the J drive
	local out_dir "`root_j_dir'/03_steps/`date'/`step_num'_`step_name'"
	// directory for output on clustertmp
	local tmp_dir "`root_tmp_dir'/03_steps/`date'/`step_num'_`step_name'"
	// shell file
	local shell_file "SHELL"
	// functional
	local functional "meningitis"
	// metric -- measure_id for prevalence(5)
	local metric 5

	// directory for pulling files from previous step
	local pull_dir_04b "`root_tmp_dir'/03_steps/`date'/04b_outcome_prev_womort/03_outputs/01_draws"	
	local pull_dir_05b "`root_tmp_dir'/03_steps/`date'/05b_sequela_split_woseiz/03_outputs/01_draws"
	local pull_dir_07 "`root_tmp_dir'/03_steps/`date'/07_etiology_prev/03_outputs/01_draws"
	local pull_dir_08 "`root_tmp_dir'/03_steps/`date'/08_viral_prev/03_outputs/01_draws"

	// directory for standard code files
	adopath + "SHARED FUNCTIONS"

	// get demographics and locations
	get_location_metadata, location_set_id(9) clear
	keep if most_detailed == 1 & is_estimate == 1
	levelsof location_id, local(locations)
	clear
	get_demographics, gbd_team(epi) clear
	local years = r(year_ids)
	local sexes = r(sex_ids)
	local age_group_ids = r(age_group_ids)
	clear

	/* // test run
	local locations 102 207
	local years 2000 2005
	local sexes 1 */
	
	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/02_temp/02_logs/`step_num'.smcl", replace
	if !_rc local close_log 1
	else local close_log 0
	
	// check for finished.txt produced by previous step
	cap erase "`out_dir'/finished.txt"
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir "`root_j_dir'/03_steps/`date'" dirs "`step'_*", respectcase
			// remove extra quotation marks
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file "`root_j_dir'/03_steps/`date'/`dir'/finished.txt"
			if _rc {
				di "`dir' failed"
				BREAK
			}
		}
	}	

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE

	use "`in_dir'/meningitis_dimension.dta", clear
	drop if healthstate == "_parent" | grouping == "_epilepsy" | grouping == "_hearing" | grouping == "_vision"
	gen num = ""

	replace num = "05b" if grouping == "long_mild" | grouping == "long_modsev"
	replace num = "07" if grouping == "cases"
	replace num = "08" if grouping == "viral"

	foreach group in long_mild long_modsev cases viral {
		drop if grouping == "`group'" & `upload_`group'' == 0
	}
	preserve

	local n = 0
	// erase and make directory for finished checks
	! mkdir "`tmp_dir'/02_temp/01_code/checks"
	local datafiles: dir "`tmp_dir'/02_temp/01_code/checks" files "finished_*.txt"
	foreach datafile of local datafiles {
		rm "`tmp_dir'/02_temp/01_code/checks/`datafile'"
	}

	local type "check"
	// acause is etiology and cause (includes meningitis, meningitis_pneumo, etc.)
	// check to see if all these files exist, and move them to upload files
	levelsof acause, local(acause)
	foreach cause of local acause {
		cap mkdir "`tmp_dir'/03_outputs/01_draws/`cause'"
		levelsof grouping if acause == "`cause'", local(grouping)
		foreach group of local grouping {
			cap mkdir "`tmp_dir'/03_outputs/01_draws/`cause'/`group'"
			levelsof healthstate if acause == "`cause'" & grouping == "`group'", local(healthstate)
			foreach state of local healthstate {
				cap mkdir "`tmp_dir'/03_outputs/01_draws/`cause'/`group'/`state'"

				local job_name "`type'_`cause'_`group'_`state'_`step_num'"
				local slots = 4
				local mem = `slots' *2

				! qsub -P proj_custom_models -N "`job_name'" -pe multi_slot `slots' -l mem_free=`mem' "`shell_file'" ///
				"`code_dir'/`step_num'_checks.do" ///
				"`date' `step_num' `step_name' `code_dir' `in_dir' `out_dir' `tmp_dir' `root_tmp_dir' `root_j_dir' `cause' `group' `state'"
				
				di "submitting `job_name'"

				local ++ n
				sleep 1000
				restore, preserve
			}
		}
	}
		
	sleep 120000

	// wait for jobs to finish ebefore passing execution back to main step file
		local i = 0
		while `i' == 0 {
			local checks : dir "`tmp_dir'/02_temp/01_code/checks" files "finished_*.txt", respectcase
			local count : word count `checks'
			di "checking `c(current_time)': `count' of `n' jobs finished"
			if (`count' == `n') continue, break
			else sleep 60000
		}

		di "`step_num' all files renamed and moved to correct upload files"

	

// setup for parallelization
	local c = 0
	// erase and make directory for finished checks
	! mkdir "`tmp_dir'/02_temp/01_code/checks"
	local datafiles: dir "`tmp_dir'/02_temp/01_code/checks" files "finished_save*.txt"
	foreach datafile of local datafiles {
		rm "`tmp_dir'/02_temp/01_code/checks/`datafile'"
	}

	local type "save"

//	upload to DisMod (save results)
	levelsof acause, local(acause)
	foreach cause of local acause {
		levelsof grouping if acause == "`cause'", local(grouping)
		foreach group of local grouping {
			levelsof healthstate if acause == "`cause'" & grouping == "`group'", local(healthstate)
			foreach state of local healthstate {
				levelsof modelable_entity_id if acause == "`cause'" & grouping == "`group'" & healthstate == "`state'", local(id)
				clear
				
				local job_name "save_results_`id'_`c'"
				local slots = 20
				local mem = `slots' *2

				! qsub -P proj_custom_models -N "`job_name'" -pe multi_slot `slots' -l mem_free=`mem' "`shell_file'" ///
				"`code_dir'/`step_num'_parallel.do" ///
				"`date' `step_num' `step_name' `code_dir' `in_dir' `out_dir' `tmp_dir' `root_tmp_dir' `root_j_dir' `cause' `group' `state' `id'"
				
				di "submitting `job_name'"

				local ++ c
				sleep 1000
				

				restore, preserve
			}
		}
	}
	restore 
	sleep 120000

	// wait for jobs to finish ebefore passing execution back to main step file
		local i = 0
		while `i' == 0 {
			local checks : dir "`tmp_dir'/02_temp/01_code/checks" files "finished_save*.txt", respectcase
			local count : word count `checks'
			di "checking `c(current_time)': `count' of `c' jobs finished"
			if (`count' == `c') continue, break
			else sleep 60000
		}

		di "`step_num' save_results files uploaded"
	clear
	
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
					local dir: dir "$prefix/WORK/04_epi/01_database/02_data/`functional'/04_models/`gbd'/03_steps/`date'" dirs "`i'_*", respectcase
					local dir = subinstr(`"`dir'"',`"""',"",.)
					cap confirm file "$prefix/WORK/04_epi/01_database/02_data/`functional'/04_models/`gbd'/03_steps/`date'/`dir'/finished.txt"
					if _rc local write_file 0
				}
			}
			
			// write file if all steps finished
			if `write_file' {
				file open all_finished using "$prefix/WORK/04_epi/01_database/02_data/`functional'/04_models/`gbd'/03_steps/`date'/finished.txt", replace write
				file close all_finished
			}
		}
		
	// close log if open
		if `close_log' log close
	