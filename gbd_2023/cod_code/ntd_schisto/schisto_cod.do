* GBD CoD Estimation
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
local h "FILEPATH"

* set correct data root
local data_root "FILEPATH"

* Always check variables
local prev_model_id "ADDRESS"
local ver "ADDRESS_`prev_model_id'"
local release_id "ADDRESS"
local folder_name "ADDRESS"
local meid_release_id "ADDRESS"
local create_dir 1
local rebuild 0

* Sparingly change variables-specify covariate ids here
local covariate_ids ADDRESS 

* Never change variables
local cause ntd_schisto
local cause_id ADDRESS
local clusterRootBase "FILEPATH"
local clusterRoot "FILEPATH"
local tmp_dir "FILEPATH"
local out_dir "FILEPATH"
local log_dir "FILEPATH"
local progress_dir "FILEPATH"
local interms_dir "FILEPATH"
local saveto "FILEPATH"
local minAge 4
local min_age `minAge'
local get_model_results gbd_team(ADDRESS) version_id(`prev_model_id') measure_id(5) release_id(`meid_release_id')
local saveto "FILEPATH"

* Make and Clear Directories
* set up base directories on shared

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
	log using "FILEPATH/submit_schisto_cod_log_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir

	tempfile model

/*====================================================================
                        1: Setup Custom Model
====================================================================*/


*--------------------1.1: Load Custom CoD Functions

run FILEPATH/build_cod_dataset.ado
run FILEPATH/select_xforms.ado
run FILEPATH/run_best_model.ado
run FILEPATH/process_predictions.ado
run FILEPATH/get_location_metadata.ado
run FILEPATH/get_demographics.ado
run FILEPATH/get_population.ado

/*====================================================================
                        2: Create the Dataset
====================================================================*/

if `rebuild' == 1 {

*--------------------2.1: Set Dataset Settings
*--------------------2.2: Build CoD Dataset

	*** CREATE THE DATASET ***
	** use this if want new covariates or if there's been a data refresh
	build_cod_dataset `cause_id', covariate_ids(`covariate_ids') get_model_results(`get_model_results') saveto(`saveto') release_id(`release_id') clear
	noisily di _n "CoD dataset built"	
	use "`saveto'", clear

	get_location_metadata, location_set_id(ID) gbd_round_id(`gbd_round_id') clear
	preserve
	keep if parent_id == ID
	levelsof location_id, local(sa_subs)
	restore
	keep if parent_id == ID
	levelsof location_id, local(brazil_subs)

	// variable management

	use "`saveto'", clear

	generate lnPrev_22 = ln(prevalence_`prev_model_id'_22 + 0.000001)
	*generate logitWater = logit(water_prop)
	*generate logLDI = ln(LDI_pc)
	gen lnPrev = ln(prevalence_`prev_model_id' + 0.000001)

	** generate cases
	gen cases = prevalence_`prev_model_id' * population

	** create lag variables
	* lag 15 is the correct lag
	preserve
	local i = 15
	keep lnPrev lnPrev_22 location_id age_group_id sex_id year_id
	replace year_id = year_id + `i'
	*drop if year_id > 2019
	local expand = `i' + 1
	expand `expand' if year_id == 1980 + `i'
	bysort year_id location_id sex_id age_group_id: replace year_id = year_id - _n + 1 if year_id == 1980 + `i'
	rename lnPrev lnPrev_lag`i'
	rename lnPrev_22 lnPrev_22_lag`i'
	tempfile lag`i'
	save `lag`i'', replace
	restore
	merge m:m year_id location_id sex_id age_group_id using `lag`i'', nogen keep(1 3)

	** create the correct lag
	local lag = 15
	local i 15

	** create a South Africa covariate
	gen south_africa = 0
	foreach loc of local sa_subs {
		replace south_africa = 1 if location_id == `loc'
	}

	** pull in geographic restrictions
	preserve
	noisily di _n "At Geographic Restrictions"
	import delimited "FILEPATH/ntd_schisto_lgr.csv", clear
	keep if year_start == 1980
	rename value_endemicity endemicity
	keep location_id endemicity
	tempfile endemic

	save `endemic', replace
	restore
	** merge on to current dataset
	merge m:1 location_id using `endemic', keepusing(endemic) nogen
	replace endemic = 0 if endemic == .
	** create an endemic Brazil subnational covariate
	gen brazil_sub = 0
	foreach loc of local brazil_subs {
		replace brazil_sub = 1 if location_id == `loc' & endemic == 1
	}

	replace study_deaths=. if endemicity == 0
	replace study_deaths=. if location_id == 6
	replace study_deaths=. if location_id == 135
	save "`saveto'_post_rebuilt_dataset.dta", replace
	

use "`saveto'_post_rebuilt_dataset.dta", clear

*create subnational indicator variable for select brazil states
	
	*Ceara
	generate brazil_4755=1 if location_id==4755
	replace brazil_4755=0 if location_id!=4755
	
		
	*Goias
	generate brazil_4758=1 if location_id==4758
	replace brazil_4758=0 if location_id!=4758
	
	*Serigpe
	generate brazil_4774=1 if location_id==4774
	replace brazil_4774=0 if location_id!=4774
	
	*Sao Paoulo
	generate brazil_4775=1 if location_id==4775
	replace brazil_4775=0 if location_id!=4775
	
* Run the model
replace age_group_id = 1 if age_group_id == 388
run_best_model nbreg study_deaths south_africa brazil_sub c.lnPrev_lag15##c.haqi i.age_group_id i.sex_id, exposure(sample_size) vce(robust) difficult
replace age_group_id = 388 if age_group_id == 1

save `model', replace
save "`saveto'_coeffs.dta", replace
use "`saveto'_coeffs.dta", clear

noisily di _n "Saving file before process predictions"
	
*** SUBMIT JOBS TO CREATE DRAWS AND SAVE RESULTS ***
process_predictions `cause_id', link(ln) random(no) min_age(5) multiplier(envelope) endemic(endemic) rootDir(`out_dir')

end


*blank line to hold
