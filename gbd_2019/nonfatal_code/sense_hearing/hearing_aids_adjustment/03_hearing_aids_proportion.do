// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:		USER
// Description:	get prevalence of hearing aid use in each severity
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// set a local that contains the uwnetid of the person running the code
// Reference -- answer by USSER, available from: reference: ADDRESS
local username "`c(username)'"
local decomp "step4"

//Running interactively on cluster 
** do "FILEPATH/03_hearing_aids_proportion.do"
	local cluster_check 1
	if `cluster_check' == 1 {
		local 1		"FILEPATH"
		local 2		"FILEPATH"
		local 3		"2019_10_06"
		local 4		"03"
		local 5		"hearing_aids_proportion"
		local 6		""
		local 7		""
		local 8		"FILEPATH"
		}

// LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

	clear all
	set more off
	//set mem 2g
	//set maxvar 32000
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
		adopath + "FILEPATH"

 
	//If running locally, manually set locals (USER: I deleted or commented out ones I think will be unnecessary for running locally...if needed refer to full list below)
	if `cluster' == 0 {

		local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
        local date = subinstr("`date'", " " , "_", .)
		local step_num "03"
		local step_name "hearing_aids_proportion"
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
		cap mkdir "`root_j_dir'"
		cap mkdir "`root_j_dir'/FILEPATH"
		cap mkdir "`root_j_dir'/FILEPATH"
		cap mkdir "`root_j_dir'/FILEPATH"
		local out_dir "`root_j_dir'/FILEPATH"
		// directory for output on clustertmp
		cap mkdir "`root_tmp_dir'"
		cap mkdir "`root_tmp_dir'/FILEPATH"
		cap mkdir "`root_tmp_dir'/FILEPATH"
		cap mkdir "`root_tmp_dir'/FILEPATH"
		local tmp_dir "`root_tmp_dir'/FILEPATH"


		

	// write log if running in parallel and log is not already open
	/*
	cap noisily log using "`out_dir'/FILEPATH", replace
	if !_rc local close_log 1
	else local close_log 0
	*/
	/* check for finished.txt produced by previous step
	//di "`hold_steps'"
	//if "`hold_steps'" != "" {
		//foreach step of local hold_steps {
			//local dir: dir "`root_j_dir'/FILEPATH", respectcase
			// remove extra quotation marks
			//local dir = subinstr(`"`dir'"',`"""',"",.)
			//capture confirm file "`root_j_dir'/FILEPATH"
			//if _rc {
				//di "`dir' failed"
				//BREAK
			//}
		//}
	//}	*/
		
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE

	*** PURPOSE (from 2013) ***
		// Create the adjustment factor for hearing aids to adjust to different severities (with Norway data NHANES because from those sources we have systematic hearing aid proportion information by severity)
		// Until GBD 2017 the 65 & 80 categories were lumped together because of data limitations
		// Deafness (95+ severity) will just be 0 hearing aid assistance
		
	*** STRATEGY UPDATE (USER, 2015) ***
		//I believe that the hearing aid adjustment was done incorrectly in 2013. 
		//In short, the regression outputs (hearing aid coverage by severity) were multiplied by country specific hearing aid coverage to be country-specific hearing aid coverage by severity
			//In 2015, I am now multiplying regression outputs by the ratio of age-sex-country specific hearing aid coverage to that of Norway. 
			//I'm guessing there was systematic under-estimation of hearing aid correction in 2013 

			*Remember: this is in terms of coverage, not prevalence. Ie, outcome is of people with a given hearing loss severity, what proportion have hearing aids. Step 4 converts this to a prevalence 

			*For visualizing results of regression, see
				*FILEPATH


	//Clear previous check files
	/*
		cap mkdir "`tmp_dir'/FILEPATH"
		cap mkdir "`tmp_dir'/FILEPATH
		cap mkdir "`tmp_dir'/FILEPATH"
		local checks: dir "`tmp_dir'/FILEPATH"
		foreach check of local checks {
			rm "`tmp_dir'/FILEPATH"
			}
		*/
	run "FILEPATH"
	run "FILEPATH"
	run "FILEPATH"
	//Demographics 

		get_age_metadata, age_group_set_id(12) clear 
		keep age_group_years_start  age_group_id
		rename age_group_years_start age_start
		gen sex_id = 1
		expand 2, gen(exp)
		replace sex_id = 2 if exp == 1
		drop exp
		gen sev_thresh = 20
		foreach thresh in 35 50 65 80 {
			expand 2 if sev_thresh == 20, gen(exp)
			replace sev_thresh = `thresh' if exp == 1
			drop exp
			}
		tempfile demog
		save `demog', replace

	//Regression 
		
		
		//get_bundle_data, bundle_id(1514) decomp_step("step1") clear
		import excel using "FILEPATH", firstrow clear
		describe
		
		
		gen sex_id = 1 if sex == "Male"
		replace sex_id = 2 if sex == "Female"
		tempfile ha_sev_data
		save `ha_sev_data', replace  

***NOTE: At the end of GBD2015, USER uploaded NOR_hearing_aids_data into the custom input database - this code should pull that for GBD2016
	//note: as of 2/18/2017, I'm still pulling from the local copy because it's the same data (until NHANES is eventually added to the database)
	//USER: as of 5/7/2018 use what's in the epi db! HUNT + NHANES


			// pre-2017: For now, collapse sexes (as was done in 2013... sex coverage didn't matter much and added instability to regression)
			// GBD2017: Regression didn't not previously use analytical weights, with them sex is significant!
			// Updating for this cycle, 'aw' can be applied to the number of observations used to generate a mean. ADDRESS
			// Only collapsing years to reduce as much as possible lines where mean is 0 or 1.
		gen cases = mean*sample_size
		collapse (sum) cases sample_size, by(age_start sex_id sev_thresh)
		gen mean = cases/sample_size
		gen logit_mean = logit(mean)
			noisily regress logit_mean age_start i.sex_id i.sev_thresh [aw=sample_size] 
		use `demog', clear
		predict logit_pred_mean, xb
		predict logit_pred_se, stdp
		gen pred_mean = invlogit(logit_pred_mean)
		
				 *forvalues i = 0/999 {
                   *gen draw_`i' = invlogit(rnormal(logit_pred_mean, logit_pred_se))
         	       *}
		gen pred_se = pred_mean*(1-pred_mean)*logit_pred_se //USER: does this equation make sense? 
		sort sev age_start
		keep sev age_start pred_mean pred_se age_group_id sex_id
		order sev age_start pred_mean pred_se
		tempfile means
		save `means', replace
		levelsof age_group_id, local(ages)
		levelsof sev, local(sevs)
		levelsof sex_id, local(sexes)
		foreach sev of local sevs {
			clear
			gen age_group_id = .
			tempfile `sev'_dists
			save ``sev'_dists', replace
			foreach age of local ages {
				foreach sex of local sexes {
					use `means' if age_group_id == `age' & sev_thresh == `sev' & sex_id == `sex', clear
					local M = pred_mean
					local SE = pred_se
					local N = `M'*(1-`M')/`SE'^2
					local a = `M'*`N'
					local b = (1-`M')*`N'
					clear
					set obs 1000
					gen sev_`sev'_ = rbeta(`a',`b')
					gen num = _n-1
					gen age_group_id = `age'
					gen sex_id = `sex'
					reshape wide sev_`sev'_, i(age_group_id sex_id) j(num)
					append using ``sev'_dists'
					save ``sev'_dists', replace
				}
			}
			cap mkdir "`tmp_dir'/FILEPATH"
			save "`tmp_dir'/FILEPATH", replace //To compare 2013 to 2015
			noisily di "SAVED : `tmp_dir'/FILEPATH"
			cap mkdir "`tmp_dir'/03_outputs/"
			save "`tmp_dir'/FILEPATH", replace //will still run if running locally 
		}
	
	

	// PARALLELIZE BY LOCATION (year and sex are looped over in individual jobs)
			//Set locals of location to loop over 
			
			get_location_metadata, location_set_id(9) clear
            keep if most_detailed == 1 & is_estimate == 1
			levelsof location_id, local(location_ids)
				
			//local location_ids "58"
			//Diagnostics only 
				
				//Create special sgeoutput folder (The /epi_custom_models subfolder will keep files for 24hrs, rather than the standard 12hrs)
					//cap mkdir "FILEPATH"
					//cap mkdir "FILEPATH"
					//cap mkdir "FILEPATH"
					//cap mkdir "FILEPATH"
					//local errors_outputs `"-o "FILEPATH" -e "FILEPATH""'

			//Submit jobs for each location  
			//local n 0
			quietly {
				foreach location_id in `location_ids' {
								
								noisily di "submitting `location_id'"
								
								//qsub settings (USER: for consistency, the argument structure is the same as model_custom, with location added and hold_steps last_steps deleted)
								local jobname "step_`step_num'_loc`location_id'"
								//local project "ID"
								//local shell "FILEPATH"
								//local slots = 4
								//local mem = `slots' * 2
								//local code = "`code_dir'/`step_num'_`step_name'_parallel"
								//di `"`errors_outputs'"' //will only create errors and outputs if running in diagnostic mode 

							!qsub -N "`jobname'" -e "FILEPATH" -o "FILEPATH" -l m_mem_free=10G -l fthread=5 -l h_rt=02:00:00 -l archive=TRUE -q all.q -P "ID" "FILEPATH" "FILEPATH" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id'"	

							//local ++ n 
							}
						}
		

	// wait for parallel jobs to finish before passing execution back to main step file
	/*
	local i = 0
	while `i' == 0 {
		local checks : dir "`tmp_dir'/FILEPATH" files "FILEPATH", respectcase
		local count : word count `checks'
		di "checking `c(current_time)': `count' of `n' jobs finished"
		if (`count' == `n') continue, break
		else sleep 60000
	}
	*/
			
			

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)
/*
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
					local dir: dir "FILEPATH" dirs "`i'_*", respectcase
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
