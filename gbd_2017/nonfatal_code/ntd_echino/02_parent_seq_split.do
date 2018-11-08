/*====================================================================
project:       GBD2017
Organization:  IHME
----------------------------------------------------------------------
Creation Date:    20 Jul 2017 - 16:10:19
Do-file version:  GBD2017
Output:           Submit script to estimate sequelae prevalence for CE
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/


	version 13.1
	drop _all
	set more off

	set maxvar 32000
	if c(os) == "Unix" {
		local j "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "FILEPATH"
	}


* Directory Paths
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2017"
	*local envir (dev or prod)
	local envir = "prod"
	*local root
	local localRoot "FILEPATH"
	*cluster root
	local clusterRoot "FILEPATH"
	*directory for code
	local code_dir "FILEPATH"
	*directory for external inputs
	local in_dir "FILEPATH"
	*directory for temporary outputs on cluster to be utilized through process
	local tmp_dir "FILEPATH"
	*local temporary directory for other things:
	local local_tmp_dir "FILEPATH"
	*directory for output of draws
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"
	*directory for progress files
	local progress_dir "FILEPATH"

	* Make and Clear Directories
		capture shell mkdir "FILEPATH"
		capture shell mkdir "FILEPATH"
		capture shell mkdir "FILEPATH"
		capture shell mkdir "FILEPATH"
		capture shell mkdir "FILEPATH"

		*make all directories
		local make_dirs code in tmp local_tmp out log progress
		foreach dir in `make_dirs' {
			capture shell mkdir ``dir'_dir'
		}


	*directory for standard code files
	adopath + "FILEPATH"

	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "FILEPATH.smcl", replace
	***********************

	*SETUP TESTING MODE*
	local testing
	*Set this local to "*" if NOT testing

	*print local values
		macro dir


/*====================================================================
                        1: Get GBD Info
====================================================================*/

*--------------------1.1: Demographics

	get_demographics, gbd_team("ADDRESS") clear
		local gbdages `r(age_group_id)'
		local gbdyears `r(year_id)'
		local gbdsexes `r(sex_id)'

*--------------------1.2: Location Metadata

	get_location_metadata, location_set_id(35) clear
		keep if most_detailed==1
		levelsof location_id,local(gbdlocs)clean

*---------------------------------*
		*make skeleton
keep location_id
local num_years : list sizeof local(gbdyears)
expand `num_years'
sort location_id
egen year_id=fill(`gbdyears' `gbdyears')
tempfile skeleton
save `skeleton', replace

*--------------------1.3: Other settings

	local ce_all_prevalence_meid 1484
	local gbdround 5
	local metrics 5 6

/*====================================================================
          2: Create Structures for Submission to Qsub Estimation
====================================================================*/

*--------------------2.1:Get Draws from All-Cases Prevalence Model

	*Get best model id's
		get_best_model_versions, entity(modelable_entity) ids(`ce_all_prevalence_meid') clear
			local model_parent = model_version_id
				di in red "`Parent Model Version: `model_parent'"


/*====================================================================
                        3: Submit Estimation Script to Qsub
====================================================================*/


*--------------------3.2: Identify geographically restricted location-years

	**GEOGRAPHIC RESTRICTIONS
		import delimited "FILEPATH", clear
		gen restricted=1

		merge 1:m location_id using `skeleton', nogen keep(matched using)
			*Location-years that do not merge are endemic (not geographically restricted)
		replace restricted=0 if restricted==.

		`testing' tempfile gr_prepped
		`testing' save `gr_prepped', replace

*--------------------3.3: Submit Qsub


		*make task directory file
		`testing' use `gr_prepped', clear

		keep location_id year_id restrict
		duplicates drop
		gen id=_n
		save "`local_tmp_dir'/ce_sequelae_task_directory.dta", replace

		*get number of tasks for array job
		local num_gbdlocs: list sizeof gbdlocs
		local num_gbdyears: list sizeof gbdyears
		local num_tasks = `num_gbdlocs' * `num_gbdyears'

		*remove spaces from locals to pass through qsub
		local gbdyears = subinstr("`gbdyears'", " ", "_", .)
		local gbdages = subinstr("`gbdages'", " ", "_", .)
		local gbdsexes = subinstr("`gbdsexes'", " ", "_", .)
		local metrics = subinstr("`metrics'", " ", "_", .)

		*Submit array  job
		local jobname CE_sequelaesplit
		! qsub -N `jobname' -pe multi_slot 6 -P proj_tb -m aes -t 1:`num_tasks' "FILEPATH" "`code_dir'/seq_split.do" "`gbdyears' `gbdages' `gbdsexes' `model_parent' `ce_all_prevalence_meid' `gbdround' `metrics'"

/*====================================================================
                        4: Save Results
====================================================================*/

/*
	*save the results to the database

		local meids 1485 1486 2796
		local description1485
		local description1486
		local description2796
		local filepattern "{location_id}_{year_id}.csv"

		local out_dir "FILEPATH"

		run "FILEPATH.ado"

			save_results_epi, modelable_entity_id(1485) description("`description1485'") input_dir("`out_dir'/1485") db_env(prod) input_file_pattern("`filepattern'") mark_best("True") clear


			save_results_epi, modelable_entity_id(1486) description("`description1486'") input_dir("`out_dir'/1486") db_env(prod) input_file_pattern("`filepattern'") mark_best("True") clear

			save_results_epi, modelable_entity_id(2796) description("`description2796'") input_dir("`out_dir'/2796") db_env(prod) input_file_pattern("`filepattern'") mark_best("True") clear


*/






log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

Notes:
1.
2.
3.


Version Control:
