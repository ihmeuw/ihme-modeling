* GBD 2020 CoD Estimation
*
/*====================================================================
                        0: Program set up
====================================================================*/

clear all
version 13.1
drop _all
clear mata
set more off
  
set maxvar 32000

* Pull sys user	
local user : env USER
local h "/homes/`user'"

* set correct data root
local data_root "gbd_2020"

* Always check variables
local ver "decomp_2_3_30_20_fix_age_restrictions"
local gbd_round_id 7
local decomp_step "step2"
local meid_decomp_step "iterative"
local create_dir 1
local rebuild 1

* Sparingly change variables
local covariate_ids ADDRESS1 ADDRESS2 ADDRESS3 ADDRESS4

* Never change variables
local step COD
local cause ntd_echino
local cause_id 353
local clusterRootBase "/ihme/ntds/ntd_models/`data_root'/`cause'/`step'"
local clusterRoot "`clusterRootBase'/`decomp_step'"
local tmp_dir "`clusterRoot'/tmp/"
local out_dir "`clusterRoot'/outputs"
local log_dir "`clusterRoot'/logs/"
local progress_dir "`clusterRoot'/progress/"
local interms_dir "`clusterRoot'/interms/"
local saveto "`interms_dir'`ver'"
local minAge 5
local min_age `minAge'
local get_model_results ""
local saveto "`interms_dir'`ver'"

* Make and Clear Directories
	*set up base directories on shared
if `create_dir' == 1 {

	capture shell mkdir "`clusterRootBase'"
	capture shell mkdir "`clusterRoot'"

	*make all directories
	local make_dirs tmp out log progress interms
	foreach dir in `make_dirs' {
		capture shell mkdir ``dir'_dir'
	}
}

*Directory for standard code files

	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/submit_echino_cod_log_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir

/*====================================================================
                        1: Setup Custom Model
====================================================================*/

*--------------------1.1: Load Custom CoD Functions

	run `h'/repos/ntd_models/ntd_models/custom_functions/build_cod_dataset.ado
	run `h'/repos/ntd_models/ntd_models/custom_functions/select_xforms.ado
	run `h'/repos/ntd_models/ntd_models/custom_functions/run_best_model.ado
	run `h'/repos/ntd_models/ntd_models/custom_functions/process_predictions.ado

/*====================================================================
                        2: Create the Dataset
====================================================================*/

*--------------------2.1: Set Dataset Settings

if `rebuild' == 1 {

*--------------------2.2: Build CoD Dataset

	build_cod_dataset `cause_id', covariate_ids(`covariate_ids') saveto(`saveto') gbd_round_id(`gbd_round_id') decomp_step(`decomp_step') clear
	
*--------------------2.3: Custom Variable Transformation
* TRY PLOTTING covariate against data and running median spline, linear regression, etc. 

	gen log_ldi=ln(LDI_pc)

*--------------------2.3: Save Ready-To-Go Dataset

     save "`saveto'_final_cod_dataset.dta", replace

}

/*====================================================================
                        3: Run Model
====================================================================*/

*--------------------3.1: Run the model
	
	* 2016 BEST
	 use "`saveto'_final_cod_dataset.dta", clear
     run_best_model mepoisson study_deaths i.age_group_id year_id i.super_region_id log_ldi echino_endemicity prop_pop_agg education_yrs_pc, exposure(sample_size) difficult reffects(|| location_id: R.age_group_id)
	 save "`saveto'_coeffs.dta", replace
	 use "`saveto'_coeffs.dta", clear

*--------------------3.2: Preliminary Model Testing

/*====================================================================
                        4: Process Predictions
====================================================================*/


*--------------------4.1: Use Custom CoD Function to Push Estimates to CodViz

	*local description "poisson; no region_id; interaction term agriculture and log_prop_non_urban"
	process_predictions `cause_id', link(ln) random(no) min_age(`minAge') multiplier(envelope) description("`description'") rootDir(`out_dir')


log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

Notes:
1. Exact Model from GBD 2015:
 poisson study_deaths i.age_cat i.sex sex_east year sheep_pc sanitation_prop i.echino_endem, exposure(sample_size)
*generate sex_east = 0 
*replace sex_east = 1 if inlist(region, 4, 9)
************************************************
2.Exact model from GBD 2-16:
run_best_model mepoisson study_deaths i.age_group_id year_id i.super_region_id log_ldi echino_endemicity prop_pop_agg education_yrs_pc, exposure(sample_size) difficult reffects(|| location_id: R.age_group_id)
3.


Version Control:
