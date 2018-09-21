// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:		
// Last updated:	
// Description:	Pull-in incidence draws of outcomes and add smr from neonatal encephalopathy, and prepare DisMod upload file
// 
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
	local in_dir "`root_j_dir'/02_inputs"
	// directory for output on the J drive
	local out_dir "`root_j_dir'/03_steps/`date'/`step_num'_`step_name'"
	// directory for output on clustertmp
	local tmp_dir "`root_tmp_dir'/03_steps/`date'/`step_num'_`step_name'"
	// directory for standard code files	
	adopath + "SHARED FUNCTIONS"
	// shell file
	local shell_file "SHELL"
	
	// get demographics
    get_location_metadata, location_set_id(9) clear
    keep if most_detailed == 1 & is_estimate == 1
    levelsof location_id, local(locations)
	
	get_demographics, gbd_team(epi) clear
	local years = r(year_ids)
	local sexes = r(sex_ids)
	local age_group_ids = r(age_group_ids)

	// functional
	local functional "encephalitis"
	// grouping
	local grouping "long_modsev _epilepsy"

	// set locals for encephalitis meids
	local _epilepsy_encephalitis = 2821
	local long_modsev_encephalitis = 2815

	// test run
	//local locations 69 207 
	
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

	// prepare SMR file of neonatal encephalopathy to be attached to all long_modsev iso3/year/sex
	use "`in_dir'/CP Mortality update for GBD2013.dta", clear
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

	order modelable_entity_name modelable_entity_id location_id year_id sex_id age_start age_end mean lower upper
	save "`out_dir'/02_temp/03_data/long_modsev_smr.dta", replace
	clear 

// submit parallelization to the cluster
	local n = 0
	// erase and make directory for finished checks
	! mkdir "`tmp_dir'/02_temp/01_code/checks"
	local datafiles: dir "`tmp_dir'/02_temp/01_code/checks" files "finished_*.txt"
	foreach datafile of local datafiles {
		rm "`tmp_dir'/02_temp/01_code/checks/`datafile'"
	}

	//test
	//local ihme_locs "SGP GHA"

	foreach location of local locations {
		// submit job
		//use `location_data', clear
		//keep if locations == "`location'"
		//local ihme_loc = ihme_loc_id
		local job_name "loc`location'_`step_num'"
		di "submitting `job_name'"
		local slots = 4
		local mem = `slots' * 2

		! qsub -P proj_custom_models -N "`job_name'" -pe multi_slot `slots' -l mem_free=`mem' "`shell_file'" "`code_dir'/`step_num'_parallel.do" ///
		"`date' `step_num' `step_name' `location' `code_dir' `in_dir' `out_dir' `tmp_dir' `root_tmp_dir' `root_j_dir'"

		local ++ n
		sleep 100
	}
	
	sleep 120000	

// wait for jobs to finish before passing execution back to main step file
	local i = 0
	while `i' == 0 {
		local checks : dir "`tmp_dir'/02_temp/01_code/checks" files "finished_*.txt", respectcase
		local count : word count `checks'
		di "checking `c(current_time)': `count' of `n' jobs finished"
		if (`count' == `n') continue, break
		else sleep 60000
	}

	di "Individual files created but not yet appended"

	//test run
	//local locations 69 207
	//local sexes 1
	//local years 2000 2005
	
// append all group/location/year/sex files from above to create a single DisMod upload file

	get_population, age_group_id(`age_group_ids') location_id(`locations') year_id(`years') sex_id(`sexes') clear
		drop process*
		rename year_id year_start
		gen sex = "Male"
		replace sex = "Female" if (sex_id == 2)
		drop sex_id
		gen age_start = 0
		replace age_start = 0 if age_group_id == 2
		replace age_start = 0.02 if age_group_id == 3
		replace age_start = 0.08 if age_group_id == 4
		replace age_start = 1 if age_group_id == 5
		replace age_start = (age_group_id - 5)*5 if (age_group_id > 5 & age_group_id <=20)
		replace age_start = 80 if age_group_id == 30
		replace age_start = 85 if age_group_id == 31
		replace age_start = 90 if age_group_id == 32
		replace age_start = 95 if age_group_id == 235
		gen age_end = 0.02
		replace age_end = 0.08 if age_group_id == 3
		replace age_end = 1 if age_group_id == 4
		replace age_end = 4 if age_group_id == 5
		replace age_end = age_start + 4 if (age_group_id > 5 & age_group_id <=20)
		replace age_end = 84 if age_group_id == 30
		replace age_end = 89 if age_group_id == 31
		replace age_end = 94 if age_group_id == 32
		replace age_end = 99 if age_start == 95 
		drop age_group_id
	
	tempfile population 
	save `population', replace
	clear

	foreach group of local grouping {
		foreach location of local locations {
			foreach year of local years {
				foreach sex of local sexes {
					append using "`tmp_dir'/03_outputs/01_draws/`location'/`functional'_`group'_`location'_`year'_`sex'.dta"
				}
			}
		}

		save "`tmp_dir'/03_outputs/02_summary/`functional'_`group'.dta", replace
		clear

		use "`in_dir'/GBD2016_epi_uploader_template.dta"

		append using "`tmp_dir'/03_outputs/02_summary/`functional'_`group'.dta"
		drop location_name
		merge m:1 location_id using "`in_dir'/location_id_name_2016.dta", keep (1 3) nogen
		merge m:1 location_id age_start year_start sex using `population', keep(3) nogen

		drop if year_start == 1995 | year_start == 2005

	preserve
	keep if measure=="incidence"

		replace mean = mean * population
		replace lower = lower * population
		replace upper = upper * population

		replace age_start = 40 if (age_start == 45 | age_start == 50 | age_start == 55)
		replace age_end = 59 if (age_end == 44 | age_end == 49 | age_end == 54)
		fastcollapse mean upper lower population, by(location_id year_start year_end sex age_start age_end sex_id grouping measure note_modeler modelable_entity_name modelable_entity_id level) type(sum)
		replace mean = mean/population if (age_start == 40 & age_end == 59)
		replace lower = lower/population if (age_start == 40 & age_end == 59)
		replace upper = upper/population if (age_start == 40 & age_end == 59)

		replace mean = mean/population if (age_start <40 | age_start >= 60)
		replace lower = lower/population if (age_start <40 | age_start >= 60)
		replace upper = upper/population if (age_start<40 | age_start >= 60)

	tempfile nI 
	save `nI', replace

	restore
	drop if measure == "incidence"
	append using "`nI'"
	drop population

		append using "`in_dir'/GBD2016_epi_uploader_template.dta"
		merge m:1 location_id using "`in_dir'/location_id_name_2016.dta", keep (1 3) nogen
		
		replace source_type = "Mixed or estimation"
		replace uncertainty_type = "Confidence interval"
		replace uncertainty_type_value = 95
		
		gen bundle_id = 400
		replace bundle_id = 401 if ("`group'" == "_epilepsy")
	
		tostring field_citation_value, replace
		replace field_citation_value = "Encephalitis cases subject for long-term sequela estimated from DisMod outputs of encephalitis incidence"
		replace field_citation_value = "Excess mortality estimated by SMR cerebral palsy meta-analysis" if measure == "mtexcess"
		replace field_citation_value = "Excess mortality rate obtained from DisMod outputs of epilepsy impairment envelope" if measure == "mtexcess" & note_modeler == "_epilepsy"

		replace unit_type = "Person*year"
		replace unit_value_as_published = 1

		replace urbanicity_type = "Unknown"

		replace representative_name = "Nationally representative only" if level == 3
		replace representative_name = "Representative for subnational location only" if level == 4

		drop level sex_id grouping

		sort modelable_entity_id measure location_id year_start sex age_start input_type

		replace recall_type = "Not Set"
		replace nid = 256379 if measure == "mtexcess"
		replace nid = 256428 if measure == "mtexcess" & note_modeler == "long_modsev"
		replace nid = 256337 if measure == "incidence"
		replace is_outlier = 0
		replace sex_issue = 0
		replace year_issue = 0
		replace age_issue = 0
		replace measure_issue = 0

		order seq seq_parent bundle_id

		cap mkdir "$prefix/WORK/12_bundle/`functional'/upload/`date'"
		save "$prefix/WORK/12_bundle/`functional'/upload/`date'/dm_custom_input_`functional'_`group'_`date'.dta", replace
		export excel "$prefix/WORK/12_bundle/`functional'/upload/`date'/dm_custom_input_`functional'_`group'_`date'.xlsx", ///
		firstrow(var) sheet("extraction") replace
		clear

		//this will upload using the shared function upload_epi_data
		local job_name "upload_`functional'_`group'_`date'"
		! qsub -P proj_custom_models "`job_name'" -pe multi_slot 5 -l mem_free=10 "`shell_file'" "`code_dir'/upload_encephalitis.do" ///
		"`date' `functional' `group'"
	}
	
	di in red "`step_num' DisMod upload file completed, upload via epi uploader (open and change sheet name to extraction first)"
	
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
					local dir: dir "`root_j_dir'/03_steps/`date'" dirs "`i'_*", respectcase
					local dir = subinstr(`"`dir'"',`"""',"",.)
					cap confirm file "`root_j_dir'/03_steps/`date'/`dir'/finished.txt"
					if _rc local write_file 0
				}
			}
			
			// write file if all steps finished
			if `write_file' {
				file open all_finished using "`root_j_dir'/03_steps/`date'/finished.txt", replace write
				file close all_finished
			}
		}
		
	// close log if open
		if `close_log' log close
	