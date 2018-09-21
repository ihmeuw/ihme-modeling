/*====================================================================
project:       GBD2016
Author:        USER
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 CLEAN
Output:           Estimate wasting due to STH
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
	clear mata
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
	local gbd = "gbd2016"
	*model step
	local step 03
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

	*make all directories
	local make_dirs code in tmp local_tmp out log progress_dir
	foreach dir in `make_dirs' {
		capture shell mkdir ``dir'_dir'
	}
	*clear tempfiles, logs and progress files
	foreach dir in log progress tmp {
		capture cd ``dir'_dir'
		capture shell rm *
	}

*Directory for standard code files
	adopath + FILEPATH

** Set locals from arguments passed on to job
		local locyear `1'
		local meid `2'
		local restrict `3'


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/wasting_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir


*Code Testing
	local testing
	*set equal to "*" if not testing


/*====================================================================
                        1: Non-ENDEMIC LOC-YEARS
====================================================================*/

if `restrict' == 1 {

*--------------------1.1:

	*Open zeroes file
		use "`tmp_dir'/wasting_zeroes_`meid'_`locyear'.dta"
		split locyear, p("_") destring
		rename locyear1 location_id
		rename locyear2 year_id
	
	*Format for draws upload
		gen modelable_entity_id = `meid'
		gen measure_id = 5
		keep modelable_entity_id measure_id location_id year_id age_group_id sex_id draw*
		order modelable_entity_id measure_id location_id year_id age_group_id sex_id draw*

	*Save draws
		ousheet using "`out_dir'/`meid'_prev/`location'_`year'.csv", comma replace

	

}

/*====================================================================
                        2: ENDEMIC LOC-YEARS
====================================================================*/

else if `restrict' != 1 {

*--------------------2.1: Prep Z-scores
		
		local effsize = .493826493	
		local effsize_l =	.389863021
		local effsize_u =	.584794532

		local effsize_sd = (`effsize_u' - `effsize_l') / (2*invnorm(0.975))

		forvalues i = 0/999 {
			local z_change_`i' = `effsize' + rnormal() * `effsize_sd'
		}
	
*--------------------2.2: Load Dataset

	use "`tmp_dir'/wasting_prepped_`meid'_`locyear'.dta", clear
	levelsof year_id, local(year) clean
	levelsof location_id, local(location) clean
	

	*Calculate total wasting due to heavy worm infection
		forvalues i = 0/999 {
				quietly replace sumsth_`i' = wast_`i' - normal(invnormal(wast_`i') - `z_change_`i'' * sumsth_`i') if !missing(sumsth_`i')	
				quietly replace sumsth_`i' = 0 if missing(sumsth_`i') | wast_`i' == 0
		}

		`testing' tempfile wast_split
		`testing' quietly save `wast_split', replace

	*Attribute wasting to different worm species and write draw files
		`testing' use `wast_split', clear
		
		forvalues i = 0/999 {
			quietly replace prop_`i' = prop_`i' * sumsth_`i'
			quietly replace prop_`i' = 0 if missing(prop_`i')
		}

		rename prop_* draw_*
		drop sumsth_*

		forvalue i = 0/999 {
			replace draw_`i' = 0 if age_group_id>5
		}
		
	*Export csvs of draw files
		gen measure_id = 5
		keep modelable_entity_id measure_id location_id year_id age_group_id sex_id draw*
		order modelable_entity_id measure_id location_id year_id age_group_id sex_id draw*

		ousheet using "`out_dir'/`meid'_prev/`location'_`year'.csv", comma replace

}
			



***************************
file open progress using `progress_dir'/`locyear'_`meid'.txt, text write replace
file write progress "complete"
file close progress




log close
exit
/* End of do-file */
