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
local ver FILEPATH`prev_model_id'"
local gbd_round_id 7
local decomp_step "step2"
local meid_decomp_step "iterative"
local create_dir 0
local rebuild 1

* Sparingly change variables
local covariate_ids ADDRESS ADDRESS ADDRESS ADDRESS ADDRESS ADDRESS

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
local saveto "`interms_dir'`ver'"

* Make and Clear DirectorieS
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
	log using "`log_dir'/FILEPATH`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir

	tempfile model

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

	*** CREATE THE DATASET ***
	** use this if want new covariates or if there's been a data refresh
	*build_cod_dataset `cause_id', covariate_ids(`covariate_ids') get_model_results(`get_model_results') saveto(`saveto') gbd_round_id(`gbd_round_id') decomp_step(`decomp_step') clear
	*noisily di _n "CoD dataset built"	
	use "`saveto'", clear

	get_location_metadata, location_set_id(8) gbd_round_id(`gbd_round_id') clear
	preserve
	keep if parent_id == 196
	levelsof location_id, local(sa_subs)
	restore
	keep if parent_id == 135
	levelsof location_id, local(brazil_subs)

	// variable management

	use "`saveto'", clear

	local ver "FILEPATH`prev_model_id'"
	local saveto "`interms_dir'`ver'"

	generate lnPrev_22 = ln(prevalence_`prev_model_id'_22 + 0.000001)
	generate logitWater = logit(water_prop)
	generate logLDI = ln(LDI_pc)
	gen lnPrev = ln(prevalence_`prev_model_id' + 0.000001)

	** generate cases
	gen cases = prevalence_`prev_model_id' * population

	** create lag variables
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


****************************GBD 2019 GR********************************************************
** pull in geographic restrictions
preserve
noisily di _n "At Geographic Restrictions"

import delimited FILEPATH, clear
keep if pre1980 == "pp" | pre1980 == "pa"
rename pre1980 endemicity
replace endemicity = "1" if endemicity == "pp"
replace endemicity = "0" if endemicity == "pa"
destring endemicity, replace
rename ihme_lc_id ihme_loc_id
rename loc_id location_id
rename loc_name location_name

keep ihme_loc_id location_name location_id endemicity
tempfile endemic
save `endemic', replace
restore
** merge on to current dataset
merge m:1 location_id using `endemic', keepusing(endemic) nogen
replace endemic = 0 if endemic == .

******************************************************************************************************

** create an endemic Brazil subnational covariate
	gen brazil_sub = 0
	foreach loc of local brazil_subs {
		replace brazil_sub = 1 if location_id == `loc' & endemic == 1
	}

	replace study_deaths=. if endemicity == 0
	replace study_deaths=. if location_id == 6
	replace study_deaths=. if location_id == 135
	save "`saveto'FILEPATH", replace
}
