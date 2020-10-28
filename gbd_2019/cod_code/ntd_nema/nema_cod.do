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
local ADDRESS "FILEPATH`user'"

* set correct data root
local data_root FILEPATH

* Always check variables
local prev_model_id ADDRESS
local ver "ADDRESS`prev_model_id'"
local rebuild 0
local create_dir 0
local gbd_round_id 7
local decomp_step "step2"
local meid_decomp_step "iterative"

* Sparingly change variables
local covariate_ids ADDRESS ADDRESS ADDRESS ADDRESS ADDRESS ADDRESS ADDRESS ADDRESS

* Never change variables
local step COD
local cause ADDRESS
local cause_id ADDRESS
local clusterRootBase "FILEPATH`data_root'/`cause'/`step'"
local clusterRoot "`clusterRootBase'/`decomp_step'"
local tmp_dir "`clusterRoot'FILEPATH"
local out_dir "`clusterRoot'FILEPATH"
local log_dir "`clusterRoot'FILEPATH"
local progress_dir "`clusterRoot'FILEPATH"
local interms_dir "`clusterRoot'FILEPATH"
local saveto "`interms_dir'`ver'"
local minAge 4
local min_age `minAge'
local get_model_results gbd_team(epi) model_version_id(`prev_model_id') measure_id(5) decomp_step(`meid_decomp_step')

* Make Directories
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
	log using "`log_dir'FILEPATH`date'_`time'.smcl", replace
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


/*====================================================================
                        2: Create the Dataset
====================================================================*/

if `rebuild' == 1 {

*--------------------2.1: Set Dataset Settings

*--------------------2.2: Build CoD Dataset

	build_cod_dataset `cause_id', covariate_ids(`covariate_ids') get_model_results(`get_model_results') saveto(`saveto') gbd_round_id(`gbd_round_id') decomp_step(`decomp_step') clear

*--------------------2.3: Custom Variable Transformation

	use "`saveto'", clear
	
	save "`saveto'FILEPATH", replace		
	gen logitPrev27_`prev_model_id'=logit(prevalence_`prev_model_id'_27+(.1*.0001081))
					
	*Latitude
	gen square_lat=abs_latitude^2
				
	*VANUATU PREVALENCE FIX (loc_id=30) 
		*Apply 2010 prevalence value to all years before 2010
			gen tempVan2=logitPrev27_`prev_model_id' if location_id==30 & year_id==2010
			bysort location_id: egen tempVan3=mean(tempVan2)
			replace logitPrev27_`prev_model_id'=tempVan3 if location_id==30 & year_id<2010
			drop temp*

	save "`saveto'FILEPATH", replace		
}

use "`saveto'FILEPATH", clear



/*====================================================================
                        3: Run Model
====================================================================*/

*--------------------3.1: Use Custom CoD Function to Submit Model

replace age_group_id = 1 if age_group_id == 388

run_best_model menbreg  study_deaths i.age_group_id i.sex_id logitPrev27_`prev_model_id' square_lat pop_dens_under_150_psqkm_pct education_yrs_pc prop_pop_agg , exposure(sample_size) reffects(|| location_id:)
replace age_group_id = 388 if age_group_id == 1

save "`saveto'FILEPATH", replace
use "`saveto'FILEPATH", clear

/*====================================================================
                        4: Process Predictions
====================================================================*/

*--------------------4.1: Use Custom CoD Function to Push Estimates to CodViz
process_predictions `cause_id', link(ln) random(yes) min_age(`min_age') description("`description'") multiplier(envelope) rootDir(`out_dir') saving_cause_ids(ADDRESS)
	
log close
exit
