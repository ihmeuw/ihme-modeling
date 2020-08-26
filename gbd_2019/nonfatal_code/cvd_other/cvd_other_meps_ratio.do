// Author:		USERNAME
// Description:	Use MEPS data to generate the odds ratio for cvd_other prevalence from MEPS data

// LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off

	set maxvar 32000

	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}

// Set adopath
	adopath + "FILEPATH"

set seed 5334

//Start log
capture log close
log using "FILEPATH, replace

// Set the Locals
local icd_maps "FILEPATH"
local meps_folder "FILEPATH"

local tmp_dir "FILEPATH"

get_demographics, gbd_team(epi)

** *******************************************
** 01a. Prep MEPS Data
** *******************************************
// Formatted without dots
cd "`icd_maps'"
use cause_code yll_cause using map_ICD9_detail.dta if yll_cause == "cvd_other", clear
tempfile icd_9
save `icd_9', replace

use cause_code yll_cause using map_ICD10.dta if yll_cause == "cvd_other", clear
append using `icd_9'

levelsof cause_code, local(other_icds)

clear all
tempfile meps_master
save `meps_master', replace emptyok

forvalues yr = 1996/2010 {
	local yrsub=substr("`yr'",3,2)

	// Grab PSU and strata variables from original files
	cd "FILEPATH"
	use "USA_MEPS_`yr'_HH_CONSOLIDATED.DTA", clear
	cap rename dupersid patient_id
	cap rename DUPERSID patient_id
	cap rename VARSTR`yrsub' strata
	cap rename VARPSU`yrsub' psu
	cap rename VARSTR strata
	cap rename VARPSU psu
	cap rename varstr strata
	cap rename varpsu psu
	cap rename WTDPER* weight
	cap rename POPWTF* weight
	cap rename PERWT* weight
	tempfile temp_`yr'
	save `temp_`yr'', replace
}

foreach part in OB OP ER {
	clear all
	tempfile temp_`part'
	save `temp_`part'', replace emptyok

	cd "`meps_folder'"
	use `part'/PREP/00_formatted.dta, clear

	tempfile merge_master
	save `merge_master', replace

	forvalues yr = 1996/2010 {
		local yrsub=substr("`yr'",3,2)
		use `merge_master' if year == `yr'
		merge m:1 patient_id using `temp_`yr'', keep(1 3) keepusing(strata psu weight) nogen // gap-fill strata and psu where missing

		append using `temp_`part''
		save `temp_`part'', replace
	}

	append using `meps_master'
	save `meps_master', replace
}

drop if age == .
save `meps_master', replace

// The inpatient files need to be cleaned before appending onto the others
clear all
tempfile inpatient_master
save `inpatient_master', replace emptyok

forvalues meps_year = 1996/2010 {
	local yrsub=substr("`meps_year'",3,2)
	cd "FILEPATH"
	use "USA_MEPS_`meps_year'_HOSPITAL_INPATIENT.DTA", clear
	if `meps_year' != 2010 {
		merge m:1 dupersid using USA_MEPS_`meps_year'_HH_CONSOLIDATED.DTA, keepusing(sex AGE`yrsub'X) nogen
	}
	if `meps_year' == 2010 {
		rename dupersid DUPERSID
		merge m:1 DUPERSID using USA_MEPS_`meps_year'_HH_CONSOLIDATED.DTA, keepusing(SEX AGE`yrsub'X) nogen
		rename DUPERSID dupersid
		rename SEX sex
	}

	gen year = `meps_year'
	rename AGE`yrsub'X age
	cap rename WTDPER* weight // Get the weights standardized to a single variable
	cap rename PERWT* weight
	cap rename VARPSU* psu
	cap rename VARSTR* strata
	append using `inpatient_master'
	save `inpatient_master', replace
}

rename dupersid patient_id

forvalues i = 1/4 {
	rename IPICD`i'X dx_`i'
}


// Create age bins
drop if age < 0 | age == .
replace age = 1 if age < 5 & age != 0
replace age = 5 if age >= 5 & age < 10
replace age = 10 if age >= 10 & age < 15
replace age = 15 if age >= 15 & age < 20
replace age = 20 if age >= 20 & age < 25
replace age = 25 if age >= 25 & age < 30
replace age = 30 if age >= 30 & age < 35
replace age = 35 if age >= 35 & age < 40
replace age = 40 if age >= 40 & age < 45
replace age = 45 if age >= 45 & age < 50
replace age = 50 if age >= 50 & age < 55
replace age = 55 if age >= 55 & age < 60
replace age = 60 if age >= 60 & age < 65
replace age = 65 if age >= 65 & age < 70
replace age = 70 if age >= 70 & age < 75
replace age = 75 if age >= 75 & age < 80
replace age = 80 if age >= 80

keep year sex age patient_id dx*
save `inpatient_master', replace
append using `meps_master'

// This dataset should include all relevant observations, as well as all participants who didn't have any visits (since they will be kept in the merges that are there in the hospital inpatient merging)

replace age = 80 if age >= 80

gen cvd_other = 0
forvalues i = 1/4 {
	foreach icd in `other_icds' {
		replace cvd_other = 1 if dx_`i' == "`icd'"
	}
}

// Collapse to a single person/year/age/sex observation
collapse (sum) cvd_other (mean) weight strata psu, by(patient_id sex age year)
levelsof age, local(ages)
levelsof sex, local(sexes)
levelsof year, local(years)

replace cvd_other = 1 if cvd_other >= 1
tempfile master
save `master', replace

// Let's use survey settings and the weights to generate weighted proportions of prevalence
clear all

local c 0
foreach year in `years' {
	di in red "Processing `year'"
	clear all
	tempfile append_temp_`year'
	save `append_temp_`year'', replace emptyok
	qui foreach sex in `sexes' {
		foreach age in `ages' {
			use `master' if year == `year', clear
			svyset psu [pweight = weight], strata(strata)

			svy, subpop(if sex == `sex' & age == `age'): mean cvd_other
			matrix a = e(b)
			matrix b = e(V)
			local mean = a[1,1]
			local var = b[1,1]
			local stdev = sqrt(`var')
			// di in red "Mean `mean' Std `stdev'"

			clear all
			set obs 1000
			gen draw_`year' = `mean' + rnormal() * `stdev'
			gen n = _n - 1
			gen sex = `sex'
			gen age = `age'
			append using `append_temp_`year''
			save `append_temp_`year'', replace
		}
	}
	if `c' == 0 {
		tempfile append_master
		save `append_master', replace
	}
	else {
		merge 1:1 sex age n using `append_master', nogen
		save `append_master', replace
	}
	local ++c
}

fastrowmean draw_*, mean_var_name(mean)
drop draw_*
collapse (mean) meps_prev = mean (sd) meps_sd = mean, by(age sex)


// Split the 0-1 age group to subgroups
expand 3 if age == 0
bysort sex age: gen n = _n
replace age = .01 if n == 2 & age == 0
replace age = .1 if n == 3 & age == 0
drop n

// Tostring age
tostring age, replace force usedisplay

// Delta method for SD
gen meps_var = meps_sd^2
replace meps_var = meps_var / ((meps_prev * (1-meps_prev))^2)
gen new_sd = sqrt(meps_var)

// Put it into logit space
replace meps_prev = 0.0000000000001 if meps_prev == 0
replace meps_prev = 0.999999 if meps_prev == 1
replace meps_prev = logit(meps_prev)

tempfile temp_draws
save `temp_draws', replace

destring age, replace
expand 4 if age == 80
bysort sex age: gen m = _n
replace age = 85 if m == 2 & age == 80
replace age = 90 if m == 3 & age == 80
replace age = 95 if m == 4 & age == 80
drop m

expand 1000
bysort sex age: gen n = _n - 1
gen mprev_ = meps_prev + rnormal() * new_sd

replace mprev_ = . if mprev_ == 0
replace mprev_ = invlogit(mprev_)
replace mprev_ = 0 if mprev_ == .

gen location_id = 102
gen year_id = 2005 // Even though they came from different years, we treat them all as 2005 because we want them to be bunched together vs. the US 2005 prevalence estimates for calculation.
rename sex sex_id

keep sex_id age mprev_ location_id year_id n

sort mprev_
drop n
sort location_id year_id sex_id age mprev_
by location_id year_id sex_id age: gen n = _n - 1

gen age_group_id=2 if age==0
replace age_group_id=3 if age==0.01
replace age_group_id=4 if age==0.1
replace age_group_id=5 if age==1
replace age_group_id=6 if age==5
replace age_group_id=7 if age==10
replace age_group_id=8 if age==15
replace age_group_id=9 if age==20
replace age_group_id=10 if age==25
replace age_group_id=11 if age==30
replace age_group_id=12 if age==35
replace age_group_id=13 if age==40
replace age_group_id=14 if age==45
replace age_group_id=15 if age==50
replace age_group_id=16 if age==55
replace age_group_id=17 if age==60
replace age_group_id=18 if age==65
replace age_group_id=19 if age==70
replace age_group_id=20 if age==75
replace age_group_id=30 if age==80
replace age_group_id=31 if age==85
replace age_group_id=32 if age==90
replace age_group_id=235 if age==95
drop age

save `tmp_dir'/FILEPATH, replace

** *******************************************
** Combine and Multiply Data
** *******************************************
// Here we want to get use the prevalence of cvd_other in the US in relation to the prevalence of hf in cvd_other
// We use this US-specific ratio to calculate cvd_other estimates for all other countries, using hf_prev estimates

// Grab DisMod severity draws -- combine to get the overall prevalence of HF due to cvd_other
get_draws, gbd_id_field(modelable_entity_id) gbd_id(9575) source(dismod) measure_ids(5) location_ids(102) year_ids(2005) status(best)  clear
reshape long draw_, i(location_id year_id sex_id age_group_id) j(n)

// Here, we sort by draw so that when we merge with the MEPS draws, we impose a covariance structure that relates the two sets of draws together
// We believe this is valid because we want to impose a covariance structure between the two sets of draws. While they are estimated separately, the underlying data is similar in both.
sort location_id year_id sex_id age_group_id draw_
drop n
by location_id year_id sex_id age_group_id: gen n = _n - 1

sum draw_

tempfile overall_temp
save `overall_temp', replace

merge 1:1 location_id year_id sex_id age_group_id n using `tmp_dir'/FILEPATH/meps_draws.dta, keep(3) // Only matching to the USA 2005 data
sum mprev_
gen coef_ = (mprev_ / (1 - mprev_)) / (draw_ / (1-draw_)) // Here we take the odds ratio of cvd_other divided by odds ratio of cvd_other due to HF
drop _merge mprev_ draw_ location_id year_id n

gen sorter = rnormal()
sort sex_id age_group_id sorter
by sex_id age_group_id: gen n = _n - 1
drop sorter

save `tmp_dir'/FILEPATH, replace
reshape wide coef_, i(sex_id age_group_id) j(n)
save `tmp_dir'/FILEPATH, replace

log close
