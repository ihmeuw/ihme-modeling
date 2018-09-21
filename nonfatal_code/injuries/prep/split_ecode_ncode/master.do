// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:		USERNAME
// Date:		DATE

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// PREP STATA (DON'T EDIT)
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
	if "`1'" == "" {
		local 1 FILEPATH
		local 2 FILEPATH
		local 3 DATE
		local 4 "04b"
		local 5 scaled_short_term_en_inc_by_platform

		local 8 "FILEPATH"
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
	
	// write log if running in parallel and log is not already open

	cap log using "`out_dir'/FILEPATH.smcl", replace name(log_`step_num')
	if !_rc local close_log 1
	else local close_log 0

	// check for finished.txt produced by previous step
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir "`root_j_dir'/FILEPATH/`date'" dirs "`step'_*", respectcase
			// remove extra quotation marks
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file "`root_j_dir'/FILEPATH/FILEPATH.txt"
			if _rc {
				di "`dir' failed"
				BREAK
			}
		}
	}	
	
// SETTINGS
	** how many slots is this script being run on?
	local slots 1
// Filepaths
	local diag_dir "`out_dir'/FILEPATH"
	local stepfile "`code_dir'/FILEPATH.xlsx"
	
// Import functions
	adopath + "`code_dir'/ado"
	adopath + "FILEPATH"
	start_timer, dir("`diag_dir'") name("`step_name'") slots(`slots')

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
		// SET LOCALS

		local testing = 1
		local type "epi"
		local subnational "yes"
		local debug 99
		local do_nonshock 0
			local overwrite_nonshock 0
		local do_shock 1
			local overwrite_shock 1

		local do_pops 0
		** save the countries high/low income status in the inputs for easy access in the parallel steps
		clear
			get_location_metadata, location_set_id(35) clear
			keep if most_detailed == 1
			keep location_id super_region_id super_region_name
			gen high_income=0
			replace high_income=1 if super_region_name=="High-income"
			keep location_id high_income 
		save "`out_dir'/FILEPATH.dta", replace
				
		** save the parent/child relationships for pulling when scaling incidence post-EN matrix for easy access during jobs
		import excel using "`in_dir'/FILEPATH.xlsx", firstrow clear
		rename acause child_model
		gen parent_model=subinstr(DisModmodels, "Child of ", "", .) if regexm(DisModmodels, "Child of")
		replace DisModmodels="Single model" if child_model=="inj_disaster" | child_model=="inj_war"
		replace parent_model=child_model if DisModmodels=="Single model"
		keep if parent_model!=""
		keep child_model parent_model DisModmodels
		save "`out_dir'/FILEPATH.dta", replace
		
		
*** ********************* STEP 1: APPLY EN Matrix to nonshock E-codes for the normal GBD country/years/sexes ********************************
		local code "`step_name'/FILEPATH.do" 

	// set mem/slots and create job checks directory
	// Test on Dev, hard-code values from test environment (a single node totally filled with these jobs).
		// Maximum memory used by job via qstat -j job_id, under maxvmem.
		local ideal_mem = ceil(6.061)
		// Calculate ideal slots based on monitoring %CPU usage test.
		local cpu_usage_ideal = 80
		local cpu_usage_total = 140
		local slots_node_total = 64
		local slots_allocated = 4
		local ideal_jobs = (`cpu_usage_ideal'/`cpu_usage_total') * (`slots_node_total'/`slots_allocated')
		local ideal_slots = round(`slots_node_total'/`ideal_jobs')
		// Use whichever is higher - ideal allocation based on memory of %CPU usage.
		if `ideal_mem'/2 > `ideal_slots' local ideal_slots = `ideal_mem'/2
		
	// submit jobs
		local n 0
		// Pull demographics to loop over
		get_demographics , gbd_team(epi) clear

		local year_ids `r(year_ids)'
		local age_group_ids `r(age_group_ids)'
		local location_ids `r(location_ids)'
		local sex_ids `r(sex_ids)'

		clear
		if `do_pops' == 1 {
			get_population, year_id(`year_ids') location_id(`location_ids') sex_id(`sex_ids') age_group_id(`age_group_ids')
			// Save populations file for use by jobs
			save "`out_dir'FILEPATH.dta", replace
		}

			if `do_nonshock' == 1 {
				if `testing' == 1{
					local year_ids 
					local location_ids 
					local sex_ids 1 2
				}
			foreach location_id of local location_ids {
				foreach year_id of local year_ids {
					foreach sex_id of local sex_ids {
						if `overwrite_nonshock' == 0 {
							quietly capture confirm file "FILEPATH"
							if _rc != 0 {
								! qsub  -P proj_injuries -N _`step_num'_`location_id'_`year_id'_`sex_id' -pe multi_slot `ideal_slots' -l mem_free=`ideal_mem' -p -2 "`code_dir'/FILEPATH.sh" "`code_dir'/`code'" "`repo'/`gbd'/`code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `year_id' `sex_id'"
								local n = `n' + 1
							}
						}
						if `overwrite_nonshock' == 1 {
							! qsub  -P proj_injuries -N _`step_num'_`location_id'_`year_id'_`sex_id' -pe multi_slot `ideal_slots' -l mem_free=`ideal_mem' -p -2 "`code_dir'/FILEPATH.sh" "`code_dir'/`code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `year_id' `sex_id' extra_arg"
							local n = `n' + 1
						}
					}
				}
			}		
		}

		*** ********************* STEP 2: APPLY EN Matrix to Shock E-codes for the all country/year/sexes that have ********************************
		
		local ideal_slots 8
		local platforms = "inp otp"
		local shock_code "`step_name'/FILEPATH.do"
		if `do_shock' == 1 {
			if `testing' == 1 {
				local location_ids 44737
				local platforms "inp"
				local sex_ids 1
			}

			if `redo' == 1 {
				local location_ids `redo_locs_shocks'
			}
		foreach location_id of local location_ids {
			foreach platform of local platforms {
				foreach sex_id of local sex_ids {
							! qsub -P proj_injuries_2 -N _`step_num'_`location_id'_`platform'_`sex_id' -pe multi_slot `ideal_slots' -l mem_free=`ideal_mem' -p -2 "`code_dir'/FILEPATH.sh" "`code_dir'/`shock_code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `sex_id' `platform'"
				}
			}
		}			
	}


** ***********************************************************
// Write check files
** ***********************************************************

// write check file to indicate step has finished

	file open finished using "`out_dir'/FILEPATH.txt", replace write
	file close finished
		
// end the timer for this step & sum total time taken
	end_timer, dir("`diag_dir'") name("`step_name'")
	
	// close log if open
	log close master
		
