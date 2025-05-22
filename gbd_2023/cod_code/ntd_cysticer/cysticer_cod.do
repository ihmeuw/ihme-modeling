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
  
*set maxvar 32000

*** ======================= BOILERPLATE ======================= ***
	clear all
	set more off
	set maxvar 32000
	local user : env USER
	local code_root "FILEPATH"
    local data_root "FILEPATH"

	local exec_from_args : env EXEC_FROM_ARGS
	capture confirm variable exec_from_args, exact
	if "`exec_from_args'" == "True" {
		local params_dir 		`2'
		local draws_dir 		`4'
		local interms_dir		`6'
		local logs_dir 			`8'
		local release_id		`10'
	}
	else {
		local params_dir "FILEPATH"
		local draws_dir "FILEPATH"
		local interms_dir "FILEPATH"
		local logs_dir "FILEPATH"
		local release_id ADDRESS
	}

* Pull sys user	
local user : env USER
local h "FILEPATH"

* Always check variables
local ver "ADDRESS"
local release_id ADDRESS
local create_dir 1
local rebuild 1
* Sparingly change variables
local covariate_ids IDS

* Never change variables
local cause ntd_cysticer
local cause_id ID
local clusterRoot "`draws_dir'"
local tmp_dir "`clusterRoot'/tmp/"
local out_dir "`clusterRoot'"
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
	log using "FILEPATH", replace
	***********************	

	*print macros in log
	macro dir

/*====================================================================
                        1: Setup Custom Model
====================================================================*/


*--------------------1.1: Load Custom CoD Functions
run "FILEPATH"


/*====================================================================
                        2: Create the Dataset
====================================================================*/

if `rebuild' == 1 {

*--------------------2.1: Set Dataset Settings

*--------------------2.2: Build CoD Dataset

	build_cod_dataset `cause_id', covariate_ids(`covariate_ids') saveto(`saveto') release_id(`release_id') clear

	use "`saveto'", clear

*--------------------2.3: Add Custom Covariate

*--------------------2.4: Custom Variable Transformation

	*Sanitation
		gen logit_sanitation=logit(sanitation_prop)	

	*Age splines
		mkspline ageS=age_group_id, cubic displayknots

		save "`saveto'_post_rebuilt_dataset.dta", replace

}

/*====================================================================
                        3: Run Model
====================================================================*/

use "`saveto'_post_rebuilt_dataset.dta", clear

*--------------------3.1: se Custom CoD Function to Submit Model

/*GBD2016 Re-run*/
*updated health_accces_capped to MCI 
run_best_model mepoisson study_deaths i.age_group_id i.super_region_id c.MCI i.sex_id  logit_sanitation religion_muslim_prop pop_dens_under_150_psqkm_pct, exposure(sample_size) difficult vce(robust) reffects(|| location_id: R.age_group_id)

save "`saveto'_coeffs.dta", replace
use "`saveto'_coeffs.dta", clear

/*====================================================================
                        4: Process Predictions
====================================================================*/

*--------------------4.1: Use Custom CoD Function to Push Estimates to CodViz

process_predictions `cause_id', link(ln) random(yes) min_age(`minAge') multiplier(envelope) description("`description'") rootDir(`out_dir')

log close
exit
/* End of do-file */




