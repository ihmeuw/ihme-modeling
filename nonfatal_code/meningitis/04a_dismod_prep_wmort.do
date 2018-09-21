// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:		
// Last updated:	
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
	local in_dir "`root_j_dir'/02_inputs"
	// directory for output on the J drive
	local out_dir "`root_j_dir'/03_steps/`date'/`step_num'_`step_name'"
	// directory for output on clustertmp
	local tmp_dir "`root_tmp_dir'/03_steps/`date'/`step_num'_`step_name'"
	// directory for standard code files	
	adopath + "SHARED FUNCTIONS"
	// shell file
	local shell_file "SHELL"

	// get location data
    get_location_metadata, location_set_id(9) clear
    keep if most_detailed == 1 & is_estimate == 1
    levelsof(location_id), local(locations)
    clear

	// get demographics
	get_demographics, gbd_team(epi) clear
	local years = r(year_ids)
	local sexes = r(sex_ids)
	local age_group_ids = r(age_group_ids)

    // functional
    local functional "meningitis"
	// etiologies
	local etiologies "meningitis_pneumo meningitis_hib meningitis_meningo meningitis_other"
	// grouping
	local grouping "long_modsev _epilepsy"

	// set locals for etiology meids
	local _epilepsy_meningitis_pneumo = 1311
	local _epilepsy_meningitis_hib = 1341
	local _epilepsy_meningitis_meningo = 1371
	local _epilepsy_meningitis_other = 1401
	local long_modsev_meningitis_pneumo = 1305
	local long_modsev_meningitis_hib = 1335
	local long_modsev_meningitis_meningo = 1365
	local long_modsev_meningitis_other = 1395

	// test run
	/*
	local locations 102 207 
	local years 2000 2005
	local sexes 1 
	*/
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
	gen etiology = ""

	order modelable_entity_name modelable_entity_id location_id year_id sex_id age_start age_end mean lower upper
	save "`out_dir'/02_temp/03_data/long_modsev_smr.dta", replace
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

		! qsub -P proj_custom_models -N "`job_name'" -pe multi_slot `slots' -l mem_free=`mem' "`shell_file'" "`code_dir'/`step_num'_parallel.do" ///
		"`date' `step_num' `step_name' `location' `code_dir' `in_dir' `out_dir' `tmp_dir' `root_tmp_dir' `root_j_dir'"

		local ++ a
		sleep 100		
	}

	sleep 120000

// wait for jobs to finish ebefore passing execution back to main step file
	local b = 0
	while `b' == 0 {
		local checks : dir "`tmp_dir'/02_temp/01_code/checks" files "finished_loc*.txt", respectcase
		local count : word count `checks'
		di "checking `c(current_time)': `count' of `a' jobs finished"
		if (`count' == `a') continue, break
		else sleep 60000
	}

	di "Individual files created but not yet appended"

// append all etiology/group/country/year/sex files from above to create a single DisMod upload file

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

	foreach etiology of local etiologies {
		foreach group of local grouping {	
			foreach location of local locations {
				foreach year of local years {
					foreach sex of local sexes {
						//di "e: `etiology' | g:`group' | l: `location' | y: `year' | s: `sex'"
						append using "`tmp_dir'/03_outputs/01_draws/`etiology'/`location'/`etiology'_`group'_`location'_`year'_`sex'.dta"
					}
				}
			}

			di "5"

			save "`tmp_dir'/03_outputs/02_summary/`etiology'_`group'.dta", replace
			clear
			
			use "`in_dir'/GBD2016_epi_uploader_template.dta"

			append using "`tmp_dir'/03_outputs/02_summary/`etiology'_`group'.dta"
			drop location_name
			merge m:1 location_id using "`in_dir'/location_id_name.dta", keep (1 3) nogen
			merge m:1 location_id age_start year_start sex using `population', keep(3) nogen

			//trying new things to make code run faster
			//cutting out 2 years out of 6 years
			drop if year_start == 1995 | year_start == 2005

		preserve
		keep if measure=="incidence"

			replace mean = mean * population
			replace lower = lower * population
			replace upper = upper * population
			
			replace age_start = 20 if (age_start == 25 | age_start == 30 | age_start == 35)
			replace age_end = 39 if (age_end == 24 | age_end == 29 | age_end == 34)
			fastcollapse mean upper lower population, by(location_id year_start year_end sex age_start age_end sex_id etiology grouping measure note_modeler modelable_entity_name modelable_entity_id level) type(sum)
			replace mean = mean/population if (age_start == 20 & age_end == 39)
			replace lower = lower/population if (age_start == 20 & age_end == 39)
			replace upper = upper/population if (age_start == 20 & age_end == 39)

			replace age_start = 40 if (age_start == 45 | age_start == 50 | age_start == 55)
			replace age_end = 59 if (age_end == 44 | age_end == 49 | age_end == 54)
			fastcollapse mean upper lower population, by(location_id year_start year_end sex age_start age_end sex_id etiology grouping measure note_modeler modelable_entity_name modelable_entity_id level) type(sum)
			replace mean = mean/population if (age_start == 40 & age_end == 59)
			replace lower = lower/population if (age_start == 40 & age_end == 59)
			replace upper = upper/population if (age_start == 40 & age_end == 59)
		
			replace age_start = 60 if (age_start == 65 | age_start == 70 | age_start == 75)
			replace age_end = 79 if (age_end == 64 | age_end == 69 | age_end == 74)
			fastcollapse mean upper lower population, by(location_id year_start year_end sex age_start age_end sex_id etiology grouping measure note_modeler modelable_entity_name modelable_entity_id level) type(sum)
			replace mean = mean/population if (age_start == 60 & age_end == 79)
			replace lower = lower/population if (age_start == 60 & age_end == 79)
			replace upper = upper/population if (age_start == 60 & age_end == 79)

			replace age_start = 80 if (age_start == 85 | age_start == 90 | age_start == 95)
			replace age_end = 99 if (age_end == 84 | age_end == 89 | age_end == 94)
			fastcollapse mean upper lower population, by(location_id year_start year_end sex age_start age_end sex_id etiology grouping measure note_modeler modelable_entity_name modelable_entity_id level) type(sum)
			replace mean = mean/population if (age_start == 80 & age_end == 99)
			replace lower = lower/population if (age_start == 80 & age_end == 99)
			replace upper = upper/population if (age_start == 80 & age_end == 99)

			replace mean = mean/population if (age_start <20)
			replace lower = lower/population if (age_start <20)
			replace upper = upper/population if (age_start<20)

		tempfile nI 
		save `nI', replace
		
		restore
		drop if measure=="incidence"
		append using `nI'
		drop population

			append using "`in_dir'/GBD2016_epi_uploader_template.dta"
			merge m:1 location_id using "`in_dir'/location_id_name.dta", keep (1 3) nogen

			replace source_type = "Mixed or estimation"
			replace uncertainty_type = "Confidence interval"
			replace uncertainty_type_value = 95

			tostring field_citation_value, replace
			replace field_citation_value = "Pneumococcal meningitis cases subject for `group' sequela estimated from DisMod outputs of meningitis incidence" if etiology == "meningitis_pneumo"
			replace field_citation_value = "HiB meningitis cases subject for `group' sequela estimated from DisMod outputs of meningitis incidence" if etiology == "meningitis_hib"
			replace field_citation_value = "Meningococcal meningitis cases subject for `group' sequela estimated from DisMod outputs of meningitis incidence" if etiology == "meningitis_meningo"
			replace field_citation_value = "Other meningitis cases subject for `group' sequela estimated from DisMod outputs of meningitis incidence" if etiology == "meningitis_other"
			replace field_citation_value = "Excess mortality estimated by SMR cerebral palsy meta-analysis" if measure == "mtexcess" & note_modeler == "long_modsev"
			replace field_citation_value = "Excess mortality rate obtained from DisMod outputs of epilepsy impairment envelope" if measure == "mtexcess" & note_modeler == "_epilepsy"

			if "`group'" == "long_medsev"{
				replace bundle_id = 31 if etiology == "meningitis_pneumo"
				replace bundle_id = 35 if etiology == "meningitis_hib"
				replace bundle_id = 39 if etiology == "meningitis_meningo"
				replace bundle_id = 43 if etiology == "meningitis_other"
			}
			if "`group'" == "_epilepsy"{
				replace bundle_id = 36 if etiology == "meningitis_hib"
				replace bundle_id = 32 if etiology == "meningitis_pneumo"
				replace bundle_id = 40 if etiology == "meningitis_meningo"
				replace bundle_id = 44 if etiology == "meningitis_other"
			}
			replace unit_type = "Person*year"
			replace unit_value_as_published = 1

			replace urbanicity_type = "Unknown"

			replace representative_name = "Nationally representative only" if level == 3
			replace representative_name = "Representative for subnational location only" if level == 4

			drop etiology level sex_id grouping

			sort modelable_entity_id measure location_id year_start sex age_start input_type

			replace recall_type = "Not Set"
			replace nid = 256379 if measure == "mtexcess" & note_modeler == "_epilepsy"
			replace nid = 256428 if measure == "mtexcess" & note_modeler == "long_modsev"
			replace nid = 256337 if measure == "incidence"
			replace is_outlier = 0
			replace sex_issue = 0
			replace year_issue = 0
			replace age_issue = 0
			replace measure_issue = 0

			cap mkdir "$prefix/WORK/12_bundle/meningitis/upload/`date'"
			save "$prefix/WORK/12_bundle/meningitis/upload/`date'/dm_custom_input_`etiology'_`group'_`date'.dta", replace
			export excel "$prefix/WORK/12_bundle/meningitis/upload/`date'/dm_custom_input_`etiology'_`group'_`date'.xlsx", firstrow(var) sheet("extraction") replace
			clear

			//this will upload using the shared function upload_epi_data
			local job_name "upload_`etiology'_`group'_`date'"

			! qsub -P proj_custom_models -N "`job_name'" -pe multi_slot 5 -l mem_free=10 "`shell_file'" "`code_dir'/upload_parallel.do" ///
			"`date' `etiology' `group'"

		}
	}
		
	di in red "`step_num' DisMod upload file completed, upload via epi uploader (open and save first)"

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
	