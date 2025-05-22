* GBD 2023 CoD Estimation
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
local FILEPATH
local FILEPATH

* set correct data root
local data_root FILEPATH

* Always check variables
local ver ADDRESS
local release_id ADDRESS
local create_dir ADDRESS
local rebuild ADDRESS
local folder_name FILEPATH

* Sparingly change variables
local covariate_ids ADDRESS

* Never change variables
local cause ADDRESS
local cause_id ADDRESS
local clusterRootBase FILEPATH
local clusterRoot FILEPATH
local tmp_dir FILEPATH
local out_dir FILEPATH
local log_dir FILEPATH
local progress_dir FILEPATH
local interms_dir FILEPATH
local saveto FILEPATH
local minAge 5
local min_age `minAge'
local FILEPATH
local saveto FILEPATH

* Make and Clear Directories
	*set up base directories on shared
if `create_dir' == 1 {

	capture shell mkdir FILEPATH
	capture shell mkdir FILEPATH

	*make all directories
	local make_dirs tmp out log progress interms
	foreach dir in `make_dirs' {
		capture shell mkdir FILEPATH
	}
}

*Directory for standard code files

	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using FILEPATH, replace
	***********************	

	*print macros in log
	macro dir

/*====================================================================
                        1: Setup Custom Model
====================================================================*/

*--------------------1.1: Load Custom CoD Functions

	run FILEPATH
	run FILEPATH
	run FILEPATH
	run FILEPATH

/*====================================================================
                        2: Create the Dataset
====================================================================*/

*--------------------2.1: Set Dataset Settings

if `rebuild' == 1 {

*--------------------2.2: Build CoD Dataset

	build_cod_dataset `cause_id', covariate_ids(`ADDRESS') saveto(`ADDRESS') release_id(`ADDRESS') clear
	
*--------------------2.3: Custom Variable Transformation


	gen log_ldi=ln(LDI_pc)

*--------------------2.3: Save Ready-To-Go Dataset

     save FILEPATH, replace

}

/*====================================================================
                        3: Run Model
====================================================================*/

*--------------------3.1: Run the model
	
	 use FILEPATH, clear
     run_best_model mepoisson study_deaths i.age_group_id year_id i.super_region_id log_ldi echino_endemicity prop_pop_agg education_yrs_pc, exposure(sample_size) difficult reffects(|| location_id: R.age_group_id)
	 save FILEPATH, replace
	 use FILEPATH, clear

*--------------------3.2: Preliminary Model Testing

/*====================================================================
                        4: Process Predictions
====================================================================*/


*--------------------4.1: Use Custom CoD Function to Push Estimates to CodViz


	process_predictions `cause_id', link(ln) random(no) min_age(`minAge') multiplier(envelope) description("`description'") rootDir(`FILEPATH')


log close
exit
/* End of do-file */


