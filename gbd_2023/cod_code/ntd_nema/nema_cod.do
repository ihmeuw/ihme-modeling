* CoD Estimation - Ascariasis
*do FILEPATH
/*====================================================================
                        0: Program set up
====================================================================*/
	
clear all
version 13.1
drop _all
clear mata
set more off
  
set maxvar 32000

ssc install regsave

* Pull sys user	
local user : env USER
local ADDRESS "FILEPATH`user'"
 
* set correct data root
local data_root "FILEPATH"

* Always check variables
*prev_model_id = the version of the ascariasis all cases MEID ADDRESS that you want to use as a covariate. It will be age-standardized
local prev_model_id "ADDRESS"
local ver "final_ADDRESS"
local create_dir 1
local release_id ADDRESS
local meid_release_id "ADDRESS"

* Sparingly change variables
local covariate_ids IDS

* Never change variables
local cause ntd_nema
local cause_id ADDRESS
local clusterRootBase "`data_root'/`cause'/ADDRESS"
local clusterRoot "`clusterRootBase'/`folder_name'"
local tmp_dir "`clusterRoot'/FILEPATH"
local out_dir "`clusterRoot'/outputs"
local log_dir "`clusterRoot'/logs/"
local progress_dir "`clusterRoot'/progress/"
local interms_dir "`clusterRoot'/interms/"
local saveto "`interms_dir'`ver'"
local minAge 4
local min_age `minAge'
local get_model_results gbd_team(ADDRESS) version_id(`prev_model_id') measure_id(5) release_id(`meid_release_id') 

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
	log using "`log_dir'submit_nema_ADDRESS_log_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir

/*====================================================================
                        1: Setup Custom Model
====================================================================*/


*--------------------1.1: Load Custom CoD Functions

	run FILEPATH/build_cod_dataset.ado
	run FILEPATH/select_xforms.ado
	run FILEPATH/run_best_model.ado
	run FILEPATH/process_predictions.ado
	run FILEPATH/get_location_metadata.ado


/*====================================================================
                        2: Create the Dataset
====================================================================*/

*--------------------2.1: Set Dataset Settings

*--------------------2.2: Build CoD Dataset

	build_cod_dataset `cause_id', covariate_ids(`covariate_ids') get_model_results(`get_model_results') saveto(`saveto') release_id(`release_id') clear

*--------------------2.3: Custom Variable Transformation

	use "`saveto'", clear
	save "`saveto'_pre_gen_logit_prev_dataset.dta", replace		
	
	gen logitPrev27_`prev_model_id'=logit(prevalence_`prev_model_id'_27+(.1*.0001081))
					
	*Latitude
	gen square_lat=abs_latitude^2
				
	*VANUATU PREVALENCE FIX (loc_id=30) 
		*Apply 2010 prevalence value to all years before 2010
			gen tempVan2=logitPrev27_`prev_model_id' if location_id==30 & year_id==2010
			bysort location_id: egen tempVan3=mean(tempVan2)
			replace logitPrev27_`prev_model_id'=tempVan3 if location_id==30 & year_id<2010
			drop temp*

	save "`saveto'_post_rebuilt_cod_dataset.dta", replace		

use "`saveto'_post_rebuilt_cod_dataset.dta", clear

preserve
noisily di _n "At Geographic Restrictions"
import delimited "FILEPATH/ntd_nema_ascar_lgr.csv", clear
keep if year_start == 1980
rename value_endemicity endemicity
keep location_id endemicity
tempfile endemic dataset_temp

save `endemic', replace
restore
** merge on to current dataset
merge m:1 location_id using `endemic', keepusing(endemic) nogen
replace endemic = 0 if endemic == .
replace study_deaths=. if endemicity == 0

save `dataset_temp', replace

* remove national points where not most detailed
get_location_metadata, location_set_id(35) release_id(`release_id') clear
keep location_id most_detailed 
merge 1:m location_id using `dataset_temp', keep(2 3) nogenerate

save "`saveto'_post_rebuilt_cod_dataset_gr.dta", replace
use "`saveto'_post_rebuilt_cod_dataset_gr.dta", clear



/*====================================================================
                        3: Run Model
====================================================================*/

*--------------------3.1: Use Custom CoD Function to Submit Model

*Review outliering decisions each time you run model

*assign temporary age group to under-6-month-olds
replace age_group_id = 1 if age_group_id == 388
*outlier from PHL subnat
replace study_deaths=. if location_id==53614
*outlier Guatemala data
replace study_deaths=. if location_id==128
*outlier STP
replace study_deaths=. if location_id==215

*remove national level data in favor of subnational points (defined by most_detailed)
replace study_deaths=. if most_detailed==0



*final model
run_best_model menbreg  study_deaths i.sex_id i.age_group_id square_lat pop_dens_under_150_psqkm_pct evi_2000_2012 logitPrev27_`prev_model_id'  , exposure(sample_size) reffects(|| location_id: R.age_group_id)

replace age_group_id = 388 if age_group_id == 1

save "`saveto'_coeffs.dta", replace
use "`saveto'_coeffs.dta", clear


/*====================================================================
                        4: Process Predictions
====================================================================*/

*--------------------4.1: Use Custom CoD Function to Push Estimates to CodViz
process_predictions `cause_id', link(log) random(yes) min_age(`min_age') description("`description'") multiplier(envelope) rootDir(`out_dir') saving_cause_ids(ID)
	
log close
exit
/* End of do-file */
