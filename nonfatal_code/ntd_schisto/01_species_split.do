// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:		USERNAME
// Description:	Split dismod output by species, and correct for geographic restrictions by species, and scale
// 				to the national level (dismod model is at level of population at risk).

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

	local root_j_dir `1'
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
	// directory for external inputs START EDITING HERE
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
    get_location_metadata, location_set_id(9) clear
    keep if most_detailed == 1 & is_estimate == 1

    // set locations for parallelization
    levelsof location_id, local(locations)
    keep ihme_loc_id location_id location_name region_name
    tempfile location_data
    save `location_data'
    clear

	local years 1990 1995 2000 2005 2010 2016
	local sexes 1 2

	// write log if running in parallel and log is not already open
	cap log using "FILEPATH.smcl", replace
	if !_rc local close_log 1
	else local close_log 0
	
	// check for finished.txt produced by previous step
	cap erase "FILEPATH/finished.txt"
	local hold_steps    
	
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
// WRITE CODE HERE

// run glems to find ratio of total cases that are haematobium vs. mansoni
		import delimited "FILEPATH.csv", clear
	// keep data points that present both
		keep if cases_h != . & cases_m != .
		drop if sample_size_h == 0 | sample_size_m == 0
	// take away year, age, sex specificity
		collapse (sum) cases_h cases_m sample_size_h sample_size_m, by(nid field_citation_value case_diagnostics_h case_diagnostics_m location_name)

	// assume a normal binomial distribution
	// create draws for both species
		forvalues i = 0/999 {
			gen prob_h_`i' = rbinomial(sample_size_h, (cases_h/sample_size_h))/sample_size_h
			replace prob_h_`i' = 0 if prob_h_`i' == .
			gen prob_m_`i' = rbinomial(sample_size_m, (cases_m/sample_size_m))/sample_size_m
			replace prob_m_`i' = 0 if prob_m_`i' == .
		}

	// calculate mean (joint probability) at the draw level
		forvalues i = 0/999 {
			gen mean_`i' = prob_h_`i' + prob_m_`i' - (prob_m_`i')*(prob_h_`i')
			drop prob_h_`i' prob_m_`i'
		}

	// get upper, lower, mean, standard error
		egen mean = rowmean(mean_*)
		egen se = rowsd(mean_*)
		drop mean_*

		gen total_cases = (mean * (mean - mean^2 - se^2))/se^2 
		replace total_cases = round(total_cases)
		** can't have 0 cases
		replace total_cases = 1 if total_cases == 0

	// clean data set
		replace cases_h = round(cases_h)
		replace cases_m = round(cases_m)
		replace total_cases = cases_h if cases_h > total_cases
		replace total_cases = cases_m if cases_m > total_cases
	// get location_id in there
		split location_name, parse("|")
		replace location_name = location_name1
		drop location_name1 location_name2
		merge m:m location_name using `location_data', nogen keep(3)
	// encode region_name
		encode(region_name), gen(reg_id)

		glm cases_m i.reg_id, family(binomial total_cases)
	// predict mean and standard error
		predict m_cases
		predict se_m, stdp
		gen prop_m = m_cases/total_cases

	// haematobium glm regression
		glm cases_h i.reg_id, family(binomial total_cases)
	// predict mean and standard error
		predict h_cases
		predict se_h, nooffset stdp
		gen prop_h = h_cases/total_cases

		collapse (mean) prop_m prop_h se_m se_h, by(region_name)
		keep region_name prop_m prop_h se_h se_m

		gen prop_total = prop_m + prop_h
		replace prop_m = prop_m/prop_total if prop_total < 1
		replace prop_h = prop_h/prop_total if prop_total < 1
		replace se_m = se_m/prop_total if prop_total < 1
		replace se_h = se_h/prop_total if prop_total < 1
		drop prop_total

	// save file 
		export delimited "FILEPATH/coinfection_region_splits.csv", replace

	// make draws folder
	! mkdir "FILEPATH"
	! mkdir "FILEPATH"
	! mkdir "FILEPATH"
	! mkdir "FILEPATH"

// parallelize code to pull in unadjusted dismod outputs and split into species specific prevalences
	local a = 0
	// erase and make directory for finished checks
	! mkdir "FILEPATHs"
	local datafiles: dir "FILEPATH" files "finished_loc*.txt"
	foreach datafile of local datafiles {
		rm "FILEPATH"
	}	

	foreach location of local locations {
		// submit job
		local job_name "loc`location'_`step_num'"
		di "submitting `job_name'"
		// di in red "location is `location'" // for tests
		local slots = 4
		local mem = `slots' * 2

		! qsub -P proj_custom_models -N "`job_name'" -pe multi_slot `slots' -l mem_free=`mem' "`shell_file'" "`code_dir'/`step_num'_parallel.do" ///
		"`date' `step_num' `step_name' `location' `code_dir' `in_dir' `out_dir' `tmp_dir' `root_tmp_dir' `root_j_dir'"

		local ++ a
		sleep 100		
	}

	sleep 120000

// wait for jobs to finish ebefore passing execution back to main step file
	local b = 0
	while `b' == 0 {
		local checks : dir "FILEPATH" files "finished_loc*.txt", respectcase
		local count : word count `checks'
		di "checking `c(current_time)': `count' of `a' jobs finished"
		if (`count' == `a') continue, break
		else sleep 60000
	}

	// run save_results
	qui run "FILEPATH.do"

	save_results, modelable_entity_id(2965) mark_best("no") env("prod") file_pattern({location_id}.csv) description("New baseline model, PAR adjustment `date'") in_dir(FILEPATH)
	save_results, modelable_entity_id(2966) mark_best("no") env("prod") file_pattern({location_id}.csv) description("New baseline model, PAR adjustment `date'") in_dir(FILEPATH)

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES 

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
					local dir: dir "FILEPATH'" dirs "`i'_*", respectcase
					local dir = subinstr(`"`dir'"',`"""',"",.)
					cap confirm file "FILEPATH.txt"
					if _rc local write_file 0
				}
			}
			
			// write file if all steps finished
			if `write_file' {
				file open all_finished using "FILEPATH.txt", replace write
				file close all_finished
			}
		}
		
	// close log if open
		if `close_log' log close
	