/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Creation Date:    10 Aug 2017 - 16:26:40
Do-file version:  GBD2017
Output:           Submit script to model mortality due to CE and process predicted estimates
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
	local gbd = "gbd2017"
	*model step
	local step COD
	*local cause
	local cause ntd_echino
	*local bundle
	local bundle /60/
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
	*set up base directories
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
	*clear tempfiles, logs and progress files
/*	foreach dir in log progress tmp {
		capture cd ``dir'_dir'
		capture shell rm *
	} */


*Directory for standard code files
	adopath + "FILEPATH"


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "FILEPATH", replace
	***********************

	*print macros in log
	macro dir

*Code Testing
	local rebuild 1
	*set equal to 0 if dataset should not be rebuilt


/*====================================================================
                        1: Setup Custom Model
====================================================================*/


*--------------------1.1: Load Custom CoD Functions

	run "FILEPATH".ado
	run "FILEPATH".ado
	run "FILEPATH".ado
	run "FILEPATH".ado


/*====================================================================
                        2: Create the Dataset
====================================================================*/

if `rebuild' == 1 {

*--------------------2.1: Set Dataset Settings

	*set arguments for build_cod_dataset function
	local cause_id 353
	local covariate_ids 142 875 1150 1087 1975 854 57 33
	local get_model_results gbd_team(ADDRESS) model_version_id(81607) measure_id(5 6)
	local saveto "FILEPATH".dta
	local minAge 5


*--------------------2.2: Build CoD Dataset

	build_cod_dataset `cause_id', covariate_ids(`covariate_ids') saveto(`saveto', replace) clear


*--------------------2.3: Custom Variable Transformation
* TRY PLOTTING covariate against data and running median spline, linear regression, etc.


	gen log_ldi=ln(LDI_pc)

	gen prop_non_urban=1-prop_urban
	gen log_prop_non_urban = ln(prop_non_urban)


*--------------------2.3: Save Ready-To-Go Dataset

	save `saveto', replace

}

/*====================================================================
                        3: Run Model
====================================================================*/

	*Open saved dataset
		if `rebuild'==0 {
			local cause_id 353
			local minAge 5
			use "FILEPATH".dta, clear
		}

*--------------------3.1: Run the model

	* 2017 BEST
	 run_best_model mepoisson study_deaths i.age_group_id year_id i.super_region_id log_ldi echino_endemicity prop_pop_agg education_yrs_pc, exposure(sample_size) difficult reffects(|| location_id: R.age_group_id)


*--------------------3.2: Preliminary Model Testing

	// calculate RMSE
        predict yhat
	gen diff = study_deaths-yhat

	histogram diff

	gen diffsquared = (study_deaths-yhat)^2
	sum(diffsquared)
	display r(sum)
	gen rmse = sqrt(`r(sum)'/43) // RMSE value
/*====================================================================
                        4: Process Predictions
====================================================================*/


*--------------------4.1: Use Custom CoD Function to Push Estimates to CodViz

	*local description "poisson; no region_id; interaction term agriculture and log_prop_non_urban"
	local description "rerun of GBD 2017 model and covariates"
	process_predictions `cause_id', link(ln) random(no) min_age(`minAge') multiplier(envelope) description("`description'")



log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

Notes:
1. Exact Model from GBD 2017:
 poisson study_deaths i.age_cat i.sex sex_east year sheep_pc sanitation_prop i.echino_endem, exposure(sample_size)

************************************************
2.Exact model from GBD 2-17:
run_best_model mepoisson study_deaths i.age_group_id year_id i.super_region_id log_ldi echino_endemicity prop_pop_agg education_yrs_pc, exposure(sample_size) difficult reffects(|| location_id: R.age_group_id)
3.


Version Control:
