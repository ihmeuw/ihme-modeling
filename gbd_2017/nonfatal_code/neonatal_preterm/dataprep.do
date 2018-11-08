***************************************************************
*** NEONATAL PRETERM-ENCEPH-SEPSIS FRAMEWORK
*** Data Preparation for CFR Regressions, Severity Regressions, and Meta-Analyses




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

if c(os) == "Unix" {
	global prefix FILEPATH
	set odbcmgr unixodbc
	local j FILEPATH
	local h "/homes/hpm7"
} 
else if c(os) == "Windows" {
	global prefix FILEPATH
	local j FILEPATH
	local h FILEPATH

}

local working_dir = FILEPATH
local data_dir = FILEPATH
local project = FILEPATH

** Opens access to Shared Functions. Required shared functions: get_location_metadata, get_epi_data
adopath + FILEPATH





********************
** ln_NMR Covariate Preparation
** Pulls NMR data from the mortality team and puts into format ready to merge with available data.
********************

use FILEPATH, clear
keep location_id region_name location_name ihme_loc_id sex year q_nn_med

gen NMR = q_nn_med * 1000
gen ln_NMR = log(NMR)
 
rename sex gender
gen sex = 1 if gender == "male"
replace sex = 2 if gender == "female" 
replace sex = 3 if gender == "both"
 
replace year = year - 0.5
drop gender q_nn_med
 
tempfile neo
save `neo', replace




********************
** Create Empty Predictions Template to store results of regressions or meta-analyses with:
**		- all locations estimated in GBD and their location metadata (e.g. associate super_region_name)
**		- all years 1950-2016 per location
**		- ln_NMR per location-year-sex
**		- developing status of location

**** 

get_location_metadata, location_set_id(9) gbd_round_id(5) clear
 
 
destring developed, replace
replace developed = 1 if parent_id == 4749 
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
expand 68
bysort location_id: gen year = 1949 + _n 
tempfile template
save `template', replace

 
merge 1:m location_id year using `neo' 
drop if _merge == 2 
drop _merge

merge m:1 location_id using `developing'
drop _merge

gen year_dev_int = year*developed

 
tempfile covariate_template
save `covariate_template', replace




import delimited "`data_dir'/dimensions_GBD2017.csv", clear 
 
tempfile dimensions
save `dimensions', replace


**********************************************************************

** For-Loop 1: Cause-Level 
 

local acause_list "neonatal_preterm"

foreach acause of local acause_list { 

	******************************************************************
	
	di in red "`acause'"

 
	di "Removing old files"
	cd "`data_dir'/02_analysis/`acause'/draws"
	local files: dir . files "`acause'*"
	foreach file of local files {
		erase `file'
	}

 
	use `dimensions', clear
	keep if acause=="`acause'"
 
	tempfile local_dimensions
	save `local_dimensions', replace		
	
 
	local out_dir "`data_dir'/01_prep/`acause'"
	local archive_dir "`out_dir'/#ARCHIVE"
	capture mkdir "`out_dir'"
	capture mkdir "`archive_dir'"
	 
	levelsof(gest_age), local(gest_age_list)
 
	if "`acause'"=="neonatal_enceph" | "`acause'" == "neonatal_sepsis" {
		local gest_age_list "none"
	}

	******************************************************************

	** For-Loop 2: Gestational Age


	foreach gest_age of local gest_age_list{
		
		use `local_dimensions', clear
		di in red "cause: `acause', gest_age: `gest_age'"
		
 
		if "`gest_age'"=="none"{
			local gest_age ""
		}
		
		tostring gest_age, replace
		replace gest_age= "" if gest_age=="."

		keep if gest_age == "`gest_age'"
 

		levelsof bundle_id, local(bundle_id_list)

		di in red "importing data"

		local x = 0


		foreach bundle_id of local bundle_id_list{
			
			di in red "bundle_id: `bundle_id'"
 

			if `bundle_id' == 351 {

				import delimited FILEPATH, clear
				destring bundle_id, replace
				destring location_id, replace
				destring year_start, replace


			} 
			else if `bundle_id' == 352 {
 
				import delimited FILEPATH, clear
 
				destring bundle_id, replace
				destring location_id, replace
				destring year_start, replace
 
			} 
			
			else if `bundle_id' == 353 {

				import delimited FILEPATH, clear
				destring bundle_id, replace
 
			} 

			if `bundle_id' != 353 & `bundle_id' != 352 & `bundle_id' != 351   {
 
				get_epi_data, bundle_id(`bundle_id') clear

			}

 
			

			capture confirm variable modelable_entity_id
			if !_rc {
 
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
 
		merge m:1 bundle_id using `local_dimensions', keep(3) nogen
 
		levelsof standard_grouping, local(standard_grouping_list)

		keep acause *grouping location_* sex year* sample_size cases covariates random_effect_levels

 
		gen year = floor((year_start+year_end)/2)
		drop year_*
 
		collapse(sum) cases sample_size, by(acause *grouping location_* sex year covariates random_effect_levels)
		gen mean = cases/sample_size
 	
		tempfile dataset
		save `dataset'

		 

		** For-Loop 3: Parameter (CFR, mild-impairment, mod/sev impairment)

		foreach standard_grouping of local standard_grouping_list {
			di in red "saving template for `standard_grouping' of `acause'"
			use `dataset', clear
			
			keep if standard_grouping == "`standard_grouping'" 
			
 
			local grouping = grouping 
			local covariates = covariates
			local random_effects = random_effect_levels
			
 
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
			
 
			save "`archive_dir'/`fname'_`timestamp'.dta", replace
			export delimited using "`archive_dir'/`fname'_`timestamp'.csv",  replace
	 
			if "`covariates'" == "meta"{
				
				* QSUB SCRIPT 
				
			}
			
 
			else if "`covariates'" == "ln_NMR"{
				
				* QSUB SCRIPT
			}


			else if "`covariates'" == "severity_regression" {
				
				* QSUB SCRIPT
				
			}
			
			di in red "analysis submitted for `grouping' of `acause'!"
		
		}
		
	
	}
	

}


di in red "All preterm-enceph-sepsis analyses submitted."
