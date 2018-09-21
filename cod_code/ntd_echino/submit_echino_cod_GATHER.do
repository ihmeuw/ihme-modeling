/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 GATHER        
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
	local gbd = "gbd2016"
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
	*directory for output of draws > on ihme/scratch
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"
	*directory for progress files
	local progress_dir "FILEPATH"

* Make and Clear Directories

	*make all directories
	local make_dirs code in tmp local_tmp out log progress_dir
	foreach dir in `make_dirs' {
		capture mkdir `dir'_dir
	}
	*clear tempfiles, logs and progress files
	foreach dir in log progress tmp {
		capture cd `dir'_dir
		capture shell rm *
	}

*Directory for standard code files
	adopath + FILEPATH


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/submit_echino_cod_log_`date'_`time'.smcl", replace
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

	run FILEPATH/build_cod_dataset.ado
	run FILEPATH/select_xforms.ado
	run FILEPATH/run_best_model.ado
	run FILEPATH/process_predictions.ado


/*====================================================================
                        2: Create the Dataset
====================================================================*/

if `rebuild' == 1 {

*--------------------2.1: Set Dataset Settings

	local cause_id 353
	local covariate_ids 142 144 875 208 1099 57 119 881 160 1087 33 463 3 17 33 100 99 845 118
	local get_model_results gbd_team(epi) model_version_id(81607) measure_id(5 6) 
	local saveto FILEPATH/echino.dta
	local minAge 5


*--------------------2.2: Build CoD Dataset

	build_cod_dataset `cause_id', covariate_ids(`covariate_ids') get_model_results(`get_model_results') saveto(`saveto', replace) clear
	

*--------------------2.3: Custom Variable Transformation

	gen log_ldi=ln(LDI_pc)

}

/*====================================================================
                        3: Run Model
====================================================================*/

	use FILEPATH/echino.dta, clear

*--------------------3.1:

	run_best_model mepoisson study_deaths i.age_group_id year_id i.super_region_id log_ldi echino_endemicity prop_pop_agg education_yrs_pc, exposure(sample_size) difficult reffects(|| location_id: R.age_group_id)


*--------------------3.2: Preliminary Model Testing

	//Test dispersion parameter in negative binomial to see if it is significant, if not use poisson
		*test [lndelta]_cons = 1\
	//Test fit of model
		*estat ic

/*====================================================================
                        4: Process Predictions
====================================================================*/


*--------------------4.1: Use Custom CoD Function to Push Estimates to CodViz

	local description "mepoisson CF;final mort env;i.age,year,i.SR,log_ldi,echino_endem,prop_pop_agg,education_yrs_pc"
	process_predictions `cause_id', link(ln) random(yes) min_age(`minAge') multiplier(envelope) description("`description'")



log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

Notes:
1. 
2.
3.


Version Control:


