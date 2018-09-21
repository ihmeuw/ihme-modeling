// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step pulls saved draws of hydrocele and lymphedema prevalence to convert into prevalence of ADL, and upload results
// Author:		USERNAME

/* things that this code does:

- pulls in draws of lymphedema and hydrocele
- convert the combination into proportion of ADL cases
- adjust for time period
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
*/

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

	local root_j_dir `1'
	** base directory on clustertmp
	local root_tmp_dir `2'
	** timestamp of current run (i.e. 2014_01_17)
	local date `3'
	** step number of this step (i.e. 01a)
	local step_num `4'
	** name of current step (i.e. first_step_name)
	local step_name `5'
	** step numbers of immediately anterior parent step (i.e. for step 2: 01a 01b 01c)
	local hold_steps `6'
	** step numbers for final steps that you are running in the current run (i.e. 11a 11b 11c)
	local last_steps `7'
	** directory where the code lives
	local code_dir `8'
	** directory for external inputs
	local in_dir "FILEPATH"
	** directory for output on the J drive
	local out_dir "FILEPATH"
	** directory for output on clustertmp
	local tmp_dir "FILEPATH"


	** write log if running in parallel and log is not already open
	cap log using "FILEPATH/`step_num'.smcl", replace
	if !_rc local close_log 1
	else local close_log 0

	** check for finished.txt produced by previous step
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir "FILEPATH" dirs "`step'_*", respectcase
			** remove extra quotation marks
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file "FILEPATH/finished.txt"
			if _rc {
				di "`dir' failed"
				BREAK
			}
		}
	}	

	** set adopath
	adopath + "FILEPATH"

	** set shell file
	local shell_file "FILEPATH/stata_shell.sh"

	** get locations
	get_location_metadata, location_set_id(35) clear
	keep if is_estimate == 1 & most_detailed == 1
	levelsof location_id, local(location_ids)

	*********************************************************************************************************************************************************************
	*********************************************************************************************************************************************************************
	** WRITE CODE HERE
	** make draws folders
	! mkdir "FILEPATH"
	! mkdir "FILEPATH"

	** parallelize code to pull in lymph and hydrocele draws to convert to ADL prevalence
	local a = 0
	** erase and make directory for finished checks
	! mkdir "FILEPATH"
	local datafiles: dir "FILEPATH" files "finished_loc*.txt"
	foreach datafile of local datafiles {
		rm "FILEPATH"
	}

    foreach location_id of local location_ids {
    	capture file "FILEPATH/`location'.csv"
    	if _rc {
	    	di "submitting location `location_id'"
			!qsub -N "ADL_adjustment_`location_id'" -P proj_custom_models -pe multi_slot 4 -l mem_free=8 "`shell_file'" "`code_dir'/`step_num'_parallel.do" "`location_id' `out_dir' `in_dir' `date' `tmp_dir' `step_num' `root_j_dir'"
	    	
	    	local ++ a
	    	sleep 100
	    }
    }

    sleep 120000

	** wait for jobs to finish ebefore passing execution back to main step file
	local b = 0
	while `b' == 0 {
		local checks : dir "FILEPATH" files "finished_loc*.txt", respectcase
		local count : word count `checks'
		di "checking `c(currime)': `count' of `a' jobs finished"
		if (`count' == `a') continue, break
		else sleep 60000
	}

	** run save results
	qui run "FILEPATH/save_results.do"

	save_results, modelable_entity_id(15811) mark_best("no") env("prod") file_pattern({location_id}.csv) description("USERNAME upload: `date'") in_dir(`tmp_dir'/draws)

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)

	// write check file to indicate step has finished
		file open finished using "FILEPATH/finished.txt", replace write
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
	