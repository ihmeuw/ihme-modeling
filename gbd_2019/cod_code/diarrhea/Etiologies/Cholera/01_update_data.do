/// Update all cholera input data ///
// Ensures that all data used in cholera estimation process is most up-to-date //
// This code pulls and preps input data, sources code
// to pull draws for age patterns, case fatality, diarrhea deaths, and diarrhea incidence/prevalence //
// Those lines are commented out right now though because each takes a substantial amount of time //
// and it is easier to launch them independently as jobs on the cluster //

set more off
set seed 12341234
// Set J //
global j "filepath"

//// Create a file for cholera notification data ////
import delimited "filepath/cholera_case_notification.csv", clear
gen location_ascii_name = location_name
merge m:m location_ascii_name using "filepath/ihme_loc_metadata_2017.dta", keep(3)
keep location_name location_id year_id cases super_region_name region_name super_region_id region_id
bysort location_id year_id: gen row = _n
drop if row == 2
drop row
saveold "filepath/case_notifications.dta", replace

//// Step 1: Create Draws for Cholera Data ////
import excel "filepath/cholera_proportion_data.xlsx", firstrow clear
keep nid measure location_name location_id mean lower upper standard_error is_outlier cv_inpatient cv_community year_start year_end age_start age_end sample_size cases
keep if measure=="proportion"
keep if is_outlier == 0
replace mean = 0.00001 if mean==0
replace lower = 0 if lower == .
replace lower = 0.000001 if lower ==0
replace upper = mean+standard_error*1.96 if upper==.
gen ln = logit(mean)
gen std = (logit(upper)-logit(lower))/2/1.96
gen std2 = standard_error^2 * (1/(mean*(1-mean)))^2
forval i = 1/1000 {
	gen draw_`i' = invlogit(rnormal(ln, std))
}

sort location_name nid year_start age_start
saveold "filepath/latest_cholera_data.dta", replace

//// Step 2: Make sure populations are updated ////
do "filepath/get_population.ado"
do "filepath/get_demographics.ado"

// Get age information //
import delimited "filepath/age_mapping.csv", clear
keep if order != .
tempfile ages
save `ages'

//use "filepath/ihme_loc_metadata_2019.dta", clear
import delimited "filepath/ihme_loc_metadata_2019.csv", clear
levelsof location_id, local(locs)
gen iso3 = ihme_loc_id
tempfile countries
save `countries'

//do "filepath/get_population.ado"
import delimited "filepath/population_metadata_2019.csv", clear
//get_population, year_id("all") location_id("all") age_group_id("all")  decomp_step("step3") clear

keep if year_id>=1980
rename population pop
merge m:1 age_group_id using `ages', keep(3) nogen
saveold "filepath/population_data.dta", replace

//// Step 3: Make sure covariates are updated ////
do "filepath/get_covariate_estimates.ado" 

// HAQI //
get_covariate_estimates, covariate_id(1099) decomp_step("step1") clear
		rename mean_value mean_haqi
		tempfile hs
		save `hs', replace
// Diarrhea Scalar //
get_covariate_estimates, covariate_id(740) decomp_step("step1") clear
		keep if sex_id == 2 // sex is the same though
		drop sex_id
		rename mean_value mean_sev
		tempfile sev
		save `sev', replace
// SDI //
get_covariate_estimates, covariate_id(881) decomp_step("step1") clear
	rename mean_value mean_sdi
	tempfile sdi
	save `sdi'
	
// Sanitation SEV //
get_covariate_estimates, covariate_id(866) age_group_id(5) sex_id(2) decomp_step("step1") clear
		rename mean_value mean_san
		merge 1:1 location_id year_id using `hs' , nogen
		merge 1:1 location_id year_id using `sev', nogen
		merge 1:1 location_id year_id using `sdi', nogen
		merge m:1 location_id using `countries', nogen
save "filepath/covariates.dta", replace

///// Step 4: prepare shock (epidemic) deaths /////
import delimited "filepath", clear
gen row = _n
expand 22
bysort row: gen merge_dummy = _n
gen sex_id = 1
expand 2
bysort row merge_dummy: replace sex_id = 2 if _n == 1
sort row merge_dummy sex_id

saveold "filepath/shock_deaths.dta", replace

///// Step 5: sources files to pull draws /////
// do "/filepath/00_age_pattern_draws.do"
// do "/filepath/00_cholera_get_death_draws.do"
// do "/filepath/00_cholera_get_incprev_draws.do"
// do "/filepath/00_get_cfr_draws.do"
