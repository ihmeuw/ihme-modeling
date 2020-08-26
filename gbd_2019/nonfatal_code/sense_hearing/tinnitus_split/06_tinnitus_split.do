// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:		USER
// Updated: 	USER Dec 2019
// Description:	split up severity/etiology-specific estimates with proportion that has ringing (tinnitus) 


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// set a local that contains the uwnetid of the person running the code
// Reference -- answer by Chamara, available from: reference: https://www.stata.com/statalist/archive/2013-10/msg00627.html
local username "`c(username)'"

** do "FILEPATH"
** do "FILEPATH" 
	local cluster_check 1
	if `cluster_check' == 1 {
		local 1		"FILEPATH"
		local 2		"FILEPATH"
		local 3		"2019_10_06"
		local 4		"06"
		local 5		"tinnitus_split"
		local 6		""
		local 7		""
		local 8		"FILEPATH"
		}


// LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

	clear all
	set more off
	set mem 2g
	set maxvar 32767
	set type double, perm
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
		local cluster 1 
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
		local cluster 0
	}
	// directory for standard code files
		//adopath + "FILEPATH"


	//If running locally, manually set locals (USER: I deleted or commented out ones I think will be unnecessary for running locally...if needed refer to full list below)
	if `cluster' == 0 {

		local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
        local date = subinstr("`date'", " " , "_", .)
		local step_num "06"
		local step_name "tinnitus_split"
		local hold_steps ""
		local code_dir "FILEPATH"
		local in_dir "FILEPATH"
		local root_j_dir "FILEPATH"
		local root_tmp_dir "FILEPATH"

		}

	//If running on cluster, use locals passed in by model_custom's qsub
	else if `cluster' == 1 {
		// base directory on share scratch  
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
		// directory for steps code
		local code_dir `8'
		}
	
	**Define directories 
		// directory for external inputs 
			//USER: hardcoded for now... model_custom doesn't pass it, rather by default tells us to 
			//make an input folder within root_j_dir, but I don't want to have separate input folders
			//in both FILEPATH and FILEPATH, so I'll use the input folder structure from 2013 
		local in_dir "FILEPATH"
		// directory for output on the J drive
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		local out_dir "FILEPATH"
		// directory for output on clustertmp
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		local tmp_dir "FILEPATH"


		

	// write log if running in parallel and log is not already open
	cap mkdir "FILEPATH"
	cap mkdir "FILEPATH"
	cap log using "FILEPATH", replace
	if !_rc local close_log 1
	else local close_log 0
	
	// check for finished.txt produced by previous step
	//if "`hold_steps'" != "" {
		//foreach step of local hold_steps {
			//local dir: dir "FILEPATH" dirs "FILEPATH", respectcase
			// remove extra quotation marks
			//local dir = subinstr(`"`dir'"',`"""',"",.)
			//capture confirm file "FILEPATH"
			//if _rc {
				//di "`dir' failed"
				//BREAK
			//}
		//}
	//}	
		
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE

	
		cap mkdir "FILEPATH"

		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"

		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"

//Clear previous check files
		cap mkdir ""
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH
		local checks: dir "FILEPATH"
		foreach check of local checks {
			rm "FILEPATH"
			}
			
	// PARALLELIZE BY LOCATION (year and sex are looped over in individual jobs)
			//Set locals of location to loop over 
			
				run "FILEPATH"
				get_location_metadata, location_set_id(9) gbd_round_id(6) decomp_step("step4") clear
                keep if most_detailed == 1 & is_estimate == 1
				levelsof location_id, local(location_ids)
				
				
			//Diagnostics only 
				//local location_ids "58"
				//Create special sgeoutput folder (The /epi_custom_models subfolder will keep files for 24hrs, rather than the standard 12hrs)
					cap mkdir "FILEPATH"
					cap mkdir "FILEPATH"
					cap mkdir "FILEPATH"
					cap mkdir "FILEPATH"
					local errors_outputs `"-o "FILEPATH" -e "FILEPATH""'

			//Submit jobs for each location  
			//local n 0
			quietly {
				foreach location_id in `location_ids' {
								
								noisily di "submitting `location_id'"
								
								//qsub settings (USER: for consistency, the argument structure is the same as model_custom, with location added and hold_steps last_steps deleted)
								local jobname "step_`step_num'_loc`location_id'"
								//local project "proj_custom_models"
								//local shell "FILEPATH"
								//local slots = 4
								//local mem = `slots' * 2
								//local code = "FILEPATH"
								//di `"`errors_outputs'"' //will only create errors and outputs if running in diagnostic mode 
							

							!qsub -N "`jobname'" -e "FILEPATH" -o "FILEPATH" -l m_mem_free=12G -l fthread=7 -l h_rt=05:00:00 -l archive=TRUE -q all.q -P "ID" "FILEPATH" "FILEPATH" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id'"	

							//local ++ n 
							}
						}
		

	// wait for parallel jobs to finish before passing execution back to main step file
	/*
	local i = 0
	while `i' == 0 {
		local checks : dir "FILEPATH" files "FILEPATH", respectcase
		local count : word count `checks'
		di "checking `c(current_time)': `count' of `n' jobs finished"
		if (`count' == `n') continue, break
		else sleep 60000
	}
	
			
			

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)

	// write check file to indicate step has finished
		file open finished using "FILEPATH", replace write
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
					local dir: dir "FILEPATH" dirs "FILEPATH", respectcase
					local dir = subinstr(`"`dir'"',`"""',"",.)
					cap confirm file "FILEPATH"
					if _rc local write_file 0
				}
			}
			
			// write file if all steps finished
			if `write_file' {
				file open all_finished using "FILEPATH", replace write
				file close all_finished
			}
		}
		
	// close log if open
		if `close_log' log close
	
*/
