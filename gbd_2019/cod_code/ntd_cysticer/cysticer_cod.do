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
local  "FILEPATH`user'"

* set correct data root
local data_root "FILEPATH"

* Always check variables
local ver "FILEPATH"
local gbd_round_id ADDRESS
local decomp_step "FILEPATH"
local meid_decomp_step "iterative"
local create_dir 1
local rebuild 1

* Sparingly change variables
local covariate_ids ADDRESS

* Never change variables
local step COD
local cause ntd_cysticer
local cause_id ADDRESS
local clusterRootBase "FILEPATH`data_root'/`cause'/`step'"
local clusterRoot "`clusterRootBase'/`decomp_step'"
local tmp_dir "`clusterRoot'FILEPATH"
local out_dir "`clusterRoot'FILEPATH"
local log_dir "`clusterRoot'FILEPATH"
local progress_dir "`clusterRoot'FILEPATH"
local interms_dir "`clusterRoot'FILEPATH"
local saveto "`interms_dir'`ver'"
local minAge 5
local min_age `minAge'
local get_model_results ""
local saveto "`interms_dir'`ver'"
  
* Make and Clear Directories
	*set up base directories 
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
	log using "`log_dir'FILEPATH`date'_`time'FILEPATH", replace
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
run FILEPATH
run FILEPATH
run FILEPATH


/*====================================================================
                        2: Create the Dataset
====================================================================*/

if `rebuild' == 1 {

*--------------------2.1: Set Dataset Settings

*--------------------2.2: Build CoD Dataset

	build_cod_dataset `cause_id', covariate_ids(`covariate_ids') get_model_results(`get_model_results') saveto(`saveto') gbd_round_id(`gbd_round_id') decomp_step(`decomp_step') clear

	use "`saveto'", clear


*--------------------2.3: Add Custom Covariate

*--------------------2.4: Custom Variable Transformation

	*Sanitation
		gen logit_sanitation=logit(sanitation_prop)	

	*Age splines
		mkspline ageS=age_group_id, cubic displayknots

		save "`saveto'FILEPATH", replace

}

/*====================================================================
                        3: Run Model
====================================================================*/

use "`saveto'FILEPATH", clear

*--------------------3.1: se Custom CoD Function to Submit Model

run_best_model mepoisson study_deaths i.age_group_id i.super_region_id c.MCI i.sex_id  logit_sanitation religion_muslim_prop pop_dens_under_150_psqkm_pct, exposure(sample_size) difficult vce(robust) reffects(|| location_id: R.age_group_id)

save "`saveto'FILEPATH", replace
use "`saveto'FILEPATH", clear

/*====================================================================
                        4: Process Predictions
====================================================================*/

*--------------------4.1: Use Custom CoD Function to Push Estimates to CodViz

process_predictions `cause_id', link(ln) random(yes) min_age(`minAge') multiplier(envelope) description("`description'") rootDir(`out_dir')

log close
exit
/* End of do-file */




