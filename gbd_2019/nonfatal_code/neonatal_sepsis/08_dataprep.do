***************************************************************
*** NEONATAL PRETERM-ENCEPH-SEPSIS FRAMEWORK
*** Data Preparation for CFR Regressions, Severity Regressions, and Meta-Analyses
*** Code Purpose

This code prepares the datasets that will be used in CFR regressions and long-term impairment proportion regressions/meta-analyses.

For each grouping (cfr, long_mild, long_modsev) and cause/gestational age combination (preterm_ga1, preterm_ga2, preterm_ga3, enceph, sepsis), preparation includes:

1) Deleting previous datasets
2) Pulling the data from the relevant bundle
3) Adding in, if necessary, the covariates and/or random effect data, in preparation for the regression or meta-analysis in Step 2a, 2b, or 2c
***************************************************************

*** Shared Functions:
	- get_location_metadata
	- get_epi_data


*** Inputs:
	- NMR by ENN & LNN from GBD Mortality Team: "FILEPATH/qx_results.dta" [pulled from 00_new_qx_estimates.R]

	- Epi Bundles:
		
		Neonatal Sepsis
		- 460 - Raw data about the case fatality of neonatal sepsis and other neonatal infections
		- 461 - Raw data about the proportion of babies with neonatal infections who go on to develop mild impairments
		- 462 - Raw data about the proportion of babies with neonatal infections who go on to develop moderate to severe impairments
**************************************************************

*********************
** General Prep
*********************

clear all
set more off
set maxvar 32000
version 13

local c_date = c(current_date)
local c_time = c(current_time)
local c_time_date = "`c_date'"+"_" +"`c_time'"
display "`c_time_date'"
local time_string = subinstr("`c_time_date'", ":", "_", .)
local timestamp = subinstr("`time_string'", " ", "_", .)
display "`timestamp'"


global prefix "FILEPATH"
set odbcmgr unixodbc
local j "FILEPATH"
local h "FILEPATH"


** File paths
local working_dir `c(pwd)' 
local input_dir = "FILEPATH"
local output_dir = "FILEPATH"
local project = "proj_neonatal" 

adopath + "FILEPATH"


********************
** ln_NMR Covariate Preparation
** Pulls NMR data from the mortality team and puts into format ready to merge with available data.
********************

** q_nn_med = (# of Neonatal Deaths / Total # of Live Births) / 1000
use "FILEPATH/qx_results_v2019_loc_set_9_step4.dta", clear
keep location_id region_name location_name ihme_loc_id sex year q_nn_med

** Convert NMR out of per-thousand and convert into it into log space
gen NMR = q_nn_med * 1000
gen ln_NMR = log(NMR)
drop q_nn_med

tempfile neo
save `neo', replace

********************
** Create Empty Predictions Template to store results of regressions or meta-analyses with:
**		- all locations estimated in GBD and their location metadata (e.g. associate super_region_name)
**		- all years 1950-2016 per location
**		- ln_NMR per location-year-sex
**		- developing status of location
********************

get_location_metadata, location_set_id(9) gbd_round_id(6) clear

destring developed, replace
replace developed = 1 if inlist(parent_id, 51, 62, 63, 72, 90, 4749)
replace developed = 0 if developed == . 

tempfile country_code_data
save `country_code_data', replace

preserve
keep location_id developed
duplicates drop 
tempfile developing 
save `developing', replace
restore 

keep location_id location_name location_type super_region_id super_region_name region_id region_name ihme_loc_id
expand 70
bysort location_id: gen year = 1949 + _n 
tempfile template
save `template', replace

** Merge ln_NMR covariate data and developing data into the empty template. Create year * developed status variable (either 0 or year of development)
merge 1:m location_id year using `neo' 
drop if _merge == 2 
drop _merge

merge m:1 location_id using `developing'
drop _merge

gen year_dev_int = year*developed

tempfile covariate_template
save `covariate_template', replace


********************
** The "dimensions_GBD2017" csv is used to direct the creation of prepped datasets (from the empty predictions template) and then send the prepped datasets into the correct Step 2:
**		- Step 2A: Meta-Analysis of long-term impairment (mild & mod/sev) for sepsis. The meta-analyses for mild impairment proportion & mod-sev impairment proportion are done separately.
********************

import delimited "`input_dir'/dimensions_GBD2017.csv", clear 

** Save dimesions_GBD2017 as tempfile `dimensions'
tempfile dimensions
save `dimensions', replace

**********************************************************************

** For-Loop 1: Cause-Level 

local acause_list "neonatal_sepsis"

foreach acause of local acause_list { 

	******************************************************************
	
	di in red "`acause'"

	** Delete old files
	di "Removing old files"
	local out_dir "FILEPATH/cfr_28_days"
	capture mkdir "`out_dir'"
	cd "`out_dir'"
	local files: dir . files "`acause'*"
	foreach file of local files {
		erase `file'
	}

	** Subset dimensions to only rows relevant to current acause
	use `dimensions', clear
	keep if acause=="`acause'"
	if "`acause'" == "neonatal_enceph" {
		keep if grouping == "cfr"
	}

	** Tempfile local_dimensions = only rows of dimensions_GBD2017 in current cause
	tempfile local_dimensions
	save `local_dimensions', replace		
		 
	levelsof(gest_age), local(gest_age_list)
	
	** Enceph and sepsis don't have gestational age splits, but we want to loop through every gestational age (for the sake of preterm), so we temporarily generate a gestational age 
	if "`acause'"=="neonatal_enceph" | "`acause'" == "neonatal_sepsis" {
		local gest_age_list "none"
	}

	******************************************************************

	** For-Loop 2: Gestational Age

	foreach gest_age of local gest_age_list{
		
		use `local_dimensions', clear
		di in red "cause: `acause', gest_age: `gest_age'"
		
		** Revert gest_age back to nothing for sepsis & encephalopathy
		if "`gest_age'"=="none"{
			local gest_age ""
		}
		
		tostring gest_age, replace
		replace gest_age= "" if gest_age=="."

		keep if gest_age == "`gest_age'"
		
		
		********************
		** Use get_epi_data to retrieve the data stored in each bundle relevant for the current cause/gestational age and append into one dataset
		********************

		levelsof bundle_id, local(bundle_id_list)

		di in red "importing data"

		local x = 0

			if `bundle_id' != 353 & `bundle_id' != 352 & `bundle_id' != 351 & `bundle_id' != 90   {

				get_bundle_data, bundle_id(`bundle_id') decomp_step("step1") clear

			}
			

			capture confirm variable modelable_entity_id
			if !_rc {
                       di in red "modelable_entity_id already exists as column in dataset."
                       drop modelable_entity_id
            }
            
			di in red "Counting observations"
			count
			if `r(N)' == 0 {
				di "Obs = 0. No data for bundle_id `bundle_id'"
			}
			else if `x' == 0 {
				di "saving original file"
				tempfile `acause'_`gest_age'_data
				save ``acause'_`gest_age'_data', replace
			}
			else if `x' == 1 {
				di "appending subsequent files"
				append using ``acause'_`gest_age'_data', force 
				di "saving subsequent files"
				save ``acause'_`gest_age'_data', replace
			}
			local x = 1
		
		}


		********************
		** We have now appended all the data in the epi_bundles (CFR, mild impairment, moderate impairment)
		** Create grouping variable which groups all data for each analysis together
		** Other preparatory steps:
			** 1) Change sex to numeric and change data from sex-specific to non-sex specific
			** 2) Drop outliers, duplicates, datapoints with cases > sample size
			** 3) Change year from mid-year to year
			** 4) Aggregate data in same location-years into one datapoint, which is required for the regression (is it?)
		** Merge on dimensions template

		********************		
		gen grouping = ""

		** neonatal_preterm
		replace grouping = "long_mild_ga1" if bundle_id == 83
		replace grouping = "long_mild_ga2" if bundle_id == 84
		replace grouping = "long_mild_ga3" if bundle_id == 85
		replace grouping = "long_modsev_ga1" if bundle_id == 86
		replace grouping = "long_modsev_ga2" if bundle_id == 87
		replace grouping = "long_modsev_ga3" if bundle_id == 88
		replace grouping = "cfr1" if bundle_id == 351
		replace grouping = "cfr2" if bundle_id == 352
		replace grouping = "cfr3" if bundle_id == 353

		** neonatal_enceph 
		replace grouping = "cfr" if bundle_id == 337
		replace grouping = "long_mild" if bundle_id == 90
		replace grouping = "long_modsev" if bundle_id == 91

		** neonatal_sepsis
		replace grouping = "cfr" if bundle_id == 460
		replace grouping = "long_mild" if bundle_id == 461
		replace grouping = "long_modsev" if bundle_id == 462

		** change sex var to numeric
		replace sex = "1" if sex == "Male"
		replace sex = "2" if sex == "Female"
		replace sex = "3" if sex == "Both"
		destring sex, replace

		replace sex = 3 

		** Drop seq column, drop outliers, drop duplicates
		drop seq
		drop if is_outlier == 1
		duplicates drop
		drop if cases > sample_size

		replace sample_size = (mean*(1-mean))/(standard_error)^2 if sample_size == .
		replace cases = mean * sample_size if cases == .

		count if mean == . 
		di in red "acause `acause' at gestational age `gest_age' has `r(N)' missing mean values!"
		drop if mean == .

		drop if location_id == .
		
		**merge the dimensions sheet back on so we have the name/covariate information again
		di in red "merging dimensions onto data"
		merge m:1 bundle_id using `local_dimensions', keep(3) nogen

		levelsof standard_grouping, local(standard_grouping_list)

		keep acause *grouping location_* sex year* sample_size cases covariates random_effect_levels

		** Place year at midyear
		gen year = floor((year_start+year_end)/2)
		drop year_*

		** The meta-analyses and regressions cannot have more than one value per location-year. 
		collapse(sum) cases sample_size, by(acause *grouping location_* sex year covariates random_effect_levels)
		gen mean = cases/sample_size
		
		** Tempfile dataset = prepped dataset with acause, standard_grouping, grouping, location_id, location_name, sex, year, cases, sample_size, mean, global_level, location_dype, super_region_id, super_region_name, region_id, region_name, ihme_loc_id, NMR, ln_NMR, developed, year_dev_int, _merge 		
		tempfile dataset
		save `dataset'

		********************
		** Merge prepped datasets with covariate_template
		** Call next line of code based on covariates in dimensions, specifying parameter grouping, covariates, and random_effects
			** IF covariates is empty, don't call a next step.
		** Other Preparatory Steps:
			** 1) Drop any row where sex != 3 (comes from covariate template)
			** 2) Set global_level = 1 if random_effects is "global_level"
		********************

		** For-Loop 3: Parameter (CFR, mild-impairment, mod/sev impairment)

		foreach standard_grouping of local standard_grouping_list {
			di in red "saving template for `standard_grouping' of `acause'"
			use `dataset', clear
			
			keep if standard_grouping == "`standard_grouping'" 
			
			** cfr1, ln_NMR, super_region_id
			local grouping = grouping 
			local covariates = covariates
			local random_effects = random_effect_levels
			

			**necessary for long_modsev_ga2 regression
			if "`random_effects'" == "global_level"{
				gen global_level = 1
			}

			drop covariates random_effect_levels *grouping acause 
			
			di in red "official grouping name is `grouping'"
			merge 1:1 location_id year sex using `covariate_template'
			
			drop if sex != 3
			
			** save so the next script can find it
			local fname "`acause'_`grouping'_prepped"
			local prepped_dta_dir "`out_dir'/`fname'.dta"
			save "`prepped_dta_dir'", replace
			export delimited using "`out_dir'/`fname'.csv", replace
			

			** Meta-Analysis for sepsis mild_prop & sepsis modsev_prop
			** Passed: `acause' `grouping' `data_dir' `prepped_dta_dir' `timestamp' 
			if "`covariates'" == "meta"{
				
				!qsub -N "meta_`acause'_`grouping'" -P proj_neonatal -l fthread=8 -l m_mem_free=2G -l h_rt=01:00:00 -l archive -q all.q -e FILEPATH -o FILEPATH -cwd "FILEPATH/stata_shell.sh" "FILEPATH/02a_meta_analysis.do" "`acause' `grouping' FILEPATH `prepped_dta_dir' `timestamp'" 
			}
			
			di in red "analysis submitted for `grouping' of `acause'!"
		
		}
		
	
	}
	

}


di in red "All preterm-enceph-sepsis analyses submitted."
