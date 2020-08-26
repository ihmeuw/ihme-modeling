// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:		USER
// Description:	squeeze parent hearing envelope with discrete categories (0-19 dB, 20-34 dB, 35-49 dB, 50-64 dB, 80-94 dB, 95+ dB) and 35+ info
			** 1) all discrete dismod model categories should sum to 1
			** 2) 35+ information needs to be incorporated.  since we are more certain about these estimates this is reflected in the squeeze at the draw level.
			** 3) squeeze discrete categories into the 35+ 
//USER: changed filepaths to my name/directory for GBD 2019 
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// set a local that contains the uwnetid of the person running the code
// Reference -- answer by USER, available from: reference: ADDRESS
local username "`c(username)'"

//Running interactively on cluster 
** do "FILEPATH"
	local cluster_check 1
	if `cluster_check' == 1 {
		local 1		"FILEPATH"
		local 2		"FILEPATH"
		local 3		"2019_10_06"
		local 4		"02"
		local 5		"parent_squeeze"
		local 6		""
		local 7		""
		local 8		"FILEPATH"
		}
		/*
		local cluster_check 1
	if `cluster_check' == 1 {
		local 1		"FILEPATH"
		local 2		"FILEPATH"
		local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
		local date = subinstr(`"`date'"'," ","_",.)
		local 4		"02"
		local 5		"parent_squeeze"
		local 6		""
		local 7		""
		local 8		"FILEPATH"
		}
	*/
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

//If running locally, manually set locals 

	if `cluster' == 0 {

		local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
        local date = subinstr("`date'", " " , "_", .)
		local step_num "02"
		local step_name "parent_squeeze"
		local hold_steps ""
		local code_dir "FILEPATH"
		local in_dir "FILEPATH"
		local root_j_dir "FILEPATH"
		local root_tmp_dir "FILEPATH"
		
		}

	else if `cluster' == 1 {
		local root_j_dir `1'
		local root_tmp_dir `2'
		local date `3'
		local step_num `4'
		local step_name `5'
		local hold_steps `6'
		local last_steps `7'
		local code_dir `8'
		}
	//If running on cluster, use locals passed in by model_custom's qsub
	/*
	else if `cluster' == 1 {
		local root_j_dir `1'
		local root_tmp_dir `2'
		local date `3'
		local step_num `4'
		local step_name `5'
		local hold_steps `6'
		local last_steps `7'
		local code_dir `8'
		}
		
	*/
	**Define directories
		// directory for external inputs
			//USER: hardcoded for now... model_custom doesn't pass it, rather by default tells us to
			//make an input folder within root_j_dir
		local in_dir "FILEPATH"
		// directory for output on the J drive
		cap noisily mkdir "FILEPATH"
		cap noisily mkdir "FILEPATH"
		cap noisily mkdir "FILEPATH"
		cap noisily mkdir "FILEPATH"
		local out_dir "FILEPATH"
		// directory for output on clustertmp
		cap noisily mkdir "FILEPATH"
		cap noisily mkdir "FILEPATH"
		cap noisily mkdir "FILEPATH"
		cap noisily mkdir "FILEPATH"
		local tmp_dir "FILEPATH"
		//directory for temporary check files
		
		

	// write log if running in parallel and log is not already open
	cap noisily log using "FILEPATH", replace
	if !_rc local close_log 1
	else local close_log 0

	// check for finished.txt produced by previous step
	
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir "FILEPATH", respectcase
			// remove extra quotation marks
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file "FILEPATH"
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

//Clear previous check files (comment out when running on full locations)
		//cap mkdir "FILEPATH"
		//local checks: dir "FILEPATH"
		//foreach check of local checks {
			//rm "FILEPATH"
			//}
		run "FILEPATH"

		// PARALLELIZE BY LOCATION (year and sex are looped over in individual jobs)
			//Set locals of location to loop over 
			
				get_location_metadata, location_set_id(9) clear
                keep if most_detailed == 1 & is_estimate == 1
				levelsof location_id, local(location_ids) //(uncomment this line and the above for normal runs, do the below for missing locations)
			
			//local location_ids ADDRESS //test code w/ random loc
			local decomp "step4"
			//Diagnostics only 
				//local location_ids "ID"
				//Create special sgeoutput folder (The /epi_custom_models subfolder will keep files for 24hrs, rather than the standard 12hrs)
					cap mkdir "FILEPATH"
					cap mkdir "FILEPATH"
					cap mkdir "FILEPATH"
					cap mkdir "FILEPATH"
					local errors_outputs `"-o "FILEPATH" -e "FILEPATH"'
				
			//Submit jobs for each location  
			local n 0
			quietly {
				foreach location_id in `location_ids' {
								
								noisily di "submitting `location_id'"
								
								//qsub settings (USER: for consistency, the argument structure is the same as model_custom, with location added and hold_steps last_steps deleted)
								local jobname "step_`step_num'_loc`location_id'"
								local project "ADDRESS"

								//local shell "FILEPATH"
								//local shell "FILEPATH"
								//local threads = 4
								//local mem = `slots' * 2

								local code = "`code_dir'/`step_num'_`step_name'_parallel"

								//di `"`errors_outputs'"' //will only create errors and outputs if running in diagnostic mode 

								noisily di "about to qsub the 2_parent_squeeze parallel locs"

								!qsub -N "`jobname'" -e "FILEPATH" -o "FILEPATH" -l m_mem_free=5G -l fthread=5 -l h_rt=02:00:00 -l archive=TRUE -q all.q -P "ADDRESS" "FILEPATH" "FILEPATH" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id'"	
								       
							/*			
							! qsub -N "`jobname'" -P "`project'" -pe multi_slot `slots' -l mem_free=`mem' "`shell'.sh" "`code'.do" ///
								"`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id'"
							*/
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
	
		*/	
		//!qsub -N "test" -e "FILEPATH" -o "FILEPATH" -l m_mem_free=5G -l fthread=5 -l h_rt=01:00:00 -l archive=TRUE -q ADDRESS -P "ADDRESS"   
									


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)

	// write check file to indicate step has finished
	/*
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
					local dir: dir "FILEPATH", respectcase
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
	
