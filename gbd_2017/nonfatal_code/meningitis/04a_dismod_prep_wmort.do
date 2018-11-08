// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:		
// Last updated:	2/12/2016
// Description:	Pull-in incidence draws of outcomes and add smr/mtexcess from neonatal encephalopathy, and prepare DisMod upload file
// 				Number of output files: 25152 + 1 upload file				
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
	local in_dir `9'
	// directory for output on the J drive
	local out_dir "FILEPATH"
	// directory for output on clustertmp
	local tmp_dir "FILEPATH"
	// directory for standard code files	
	adopath + "FILEPATH"
	// shell file
	local shell_file "FILEPATH"

	// get locations
    get_location_metadata, location_set_id(9) clear
    keep if most_detailed == 1 & is_estimate == 1
    levelsof(location_id), local(locations)
    clear

    //test locations
	//local locations 34 527

	// get demographics
	get_demographics, gbd_team(epi) clear
	local years = r(year_id)
	local sexes = r(sex_id)
	local age_group_ids = r(age_group_id)

    // functional
    local functional "meningitis"
	// etiologies
	local etiologies "meningitis_pneumo meningitis_hib meningitis_meningo meningitis_other"
	// grouping
	local grouping "long_modsev epilepsy"

	// set locals for etiology meids
	local epilepsy_meningitis_pneumo = 1311
	local epilepsy_meningitis_hib = 1341
	local epilepsy_meningitis_meningo = 1371
	local epilepsy_meningitis_other = 1401
	local long_modsev_meningitis_pneumo = 1305
	local long_modsev_meningitis_hib = 1335
	local long_modsev_meningitis_meningo = 1365
	local long_modsev_meningitis_other = 1395

	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/02_temp/02_logs/`step_num'.smcl", replace
	if !_rc local close_log 1
	else local close_log 0


	
	// check for finished.txt produced by previous step
	cap erase "`out_dir'/finished.txt"
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir "`root_j_dir'" dirs "`step'_*", respectcase
			// remove extra quotation marks
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file "`root_j_dir'/`dir'/finished.txt"
			if _rc {
				di "`dir' failed"
				BREAK
			}
		}
	}

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
	
	// prepare SMR file of neonatal encephalopathy to be attached to all long_modsev iso3/year/sex
	use "FILEPATH", clear
	keep if cause == "NE" // neonatal encephalopathy
	drop cause parameter
	// normal distribution around the log values, so need to log transform and then inv log transform final draws
	rename ul upper
	rename ll lower
	gen modelable_entity_id = .
	gen modelable_entity_name = ""
	gen sex_id = .
	gen year_id = .
	gen location_id = .
	gen measure = "mtexcess"
	gen etiology = ""

	order modelable_entity_name modelable_entity_id location_id year_id sex_id age_start age_end mean lower upper
	save "FILEPATH", replace
	clear


	// parallelize code to pull in SMR data and all-cause mortality data to create excess mortality
	local a = 0
	// erase and make directory for finished checks
	! mkdir "`tmp_dir'/02_temp/01_code/checks"
	local datafiles: dir "`tmp_dir'/02_temp/01_code/checks" files "finished_loc*.txt"
	foreach datafile of local datafiles {
		rm "`tmp_dir'/02_temp/01_code/checks/`datafile'"
	}	
	
	foreach location of local locations {
		// submit job
		local job_name "loc`location'_`step_num'"
		di "submitting `job_name'"
		// for tests
		di in red "location is `location'" 
		local slots = 4
		local mem = `slots' * 2

		! qsub -P proj_custom_models -N "`job_name'" -o "FILEPATH" -e "FILEPATH" -pe multi_slot `slots' -l mem_free=`mem' "`shell_file'" "`code_dir'/`step_num'_parallel.do" ///
		"`date' `step_num' `step_name' `location' `code_dir' `in_dir' `out_dir' `tmp_dir' `root_tmp_dir' `root_j_dir'"

		local ++ a
		sleep 100		
	}

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
					local dir: dir "`root_j_dir'" dirs "`i'_*", respectcase
					local dir = subinstr(`"`dir'"',`"""',"",.)
					cap confirm file "`root_j_dir'/`dir'/finished.txt"
					if _rc local write_file 0
				}
			}
			
			// write file if all steps finished
			if `write_file' {
				file open all_finished using "`root_j_dir'/finished.txt", replace write
				file close all_finished
			}
		}
		
	// close log if open
		if `close_log' log close
	

