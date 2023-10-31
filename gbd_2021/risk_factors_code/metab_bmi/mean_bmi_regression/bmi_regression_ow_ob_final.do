/***********************************************************************************************************
 Author:																			
 Date: 5/29/2016		
 Last Updated by: 
 Last Updated on: 5/29/2016																			
 Project: Risk Factors BMI																	
 Purpose: Prepare BMI Input Data (Pull GBD2013, add GBD2015 literature and reports, add GBD2015 microdata)														
***********************************************************************************************************/

clear all
set more off

*log using "FILEPATH", s replace

// System stuff
/*	if c(os) == "Unix" {
		local prefix "FILEPATH"
		set more off
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local prefix "FILEPATH"
	}
*/
	adopath + "FILEPATH"

* Prepare some helper datasets
	import delimited using "FILEPATH", clear
	keep age_group_id age_start age_end
	keep if inrange(age_group_id, 5, 21)
	tempfile age_map_to_id
	save `age_map_to_id', replace

	get_location_metadata, location_set_id(22) clear
	keep if is_estimate == 1 | location_id == 4749
	keep region_name super_region_name ihme_loc_id location_id
	tempfile locs
	save `locs', replace
	
	expand 36
	bysort location_id: gen year_id = 1979 + _n
	expand 2
	bysort location_id year_id: gen sex_id = 0 + _n
	expand 13
	bysort location_id year_id sex_id: gen age_group_id = 8 + _n
	merge m:1 age_group_id using `age_map_to_id', nogen keep(3)
	
	tempfile square
	save `square', replace

**This contains the input data for bmi, obesity, and overweight
import delimited using "FILEPATH", clear
merge m:1 location_id using `locs', nogen keep(3)
merge m:1 age_group_id using `age_map_to_id', nogen keep(3)

keep if bmi_mean !=. & overweight_mean != . & obese_mean !=.

foreach var of varlist overweight_mean obese_mean {
	replace `var' = 0.005 if `var' < 0.005
	replace `var' = .995 if `var' > .995
}

replace bmi_mean = log(bmi_mean)

mixed bmi_mean overweight_mean obese_mean i.age_start i.sex_id || super_region_name: overweight_mean obese_mean || region_name: overweight_mean obese_mean || ihme_loc_id: overweight_mean obese_mean, reml

* Extract the random effects
predict re*, reffects

* Extract the fixed effects
mat coeffs = e(b)

gen intercept = coeffs[1,18] // Intercept
gen ow_coeff = coeffs[1,1] // Overweight fixed effect coefficient
gen ob_coeff = coeffs[1,2] // Obese fixed effect coefficient

* Extract age fixed effects
gen age_effect = .
local counter = 3
foreach num of numlist 20(5)80 {
	replace age_effect = coeffs[1,`counter'] if age_start == `num'
	local counter = `counter' + 1
}

* Extract sex fixed effects
gen sex_effect = .
replace sex_effect = coeffs[1,16] if sex_id == 1
replace sex_effect = coeffs[1,17] if sex_id == 2

* Create tempfiles of coefficients to merge to prediction frame
preserve
keep age_start sex_id ow_coeff ob_coeff age_effect sex_effect intercept
duplicates drop *, force
tempfile fixed_coeffs
save `fixed_coeffs', replace
restore

preserve
keep super_region_name re1 re2 re3
duplicates drop *, force
tempfile sr_res
save `sr_res', replace
restore

preserve
keep region_name re4 re5 re6
duplicates drop *, force
tempfile r_res
save `r_res', replace
restore

preserve
keep ihme_loc_id re7 re8 re9
duplicates drop *, force
tempfile c_res
save `c_res', replace
restore

use `square', clear

* Merge on the regression coefficients to use in prediction
merge m:1 age_start sex_id using `fixed_coeffs', nogen keep(3)
erase `fixed_coeffs'
merge m:1 super_region_name using `sr_res', nogen keep(3)
erase `sr_res'
merge m:1 region_name using `r_res', nogen keep(3)
erase `r_res'
merge m:1 ihme_loc_id using `c_res', keep(1 3)
erase `c_res'
replace re7 = 0 if _merge == 1
replace re8 = 0 if _merge == 1
replace re9 = 0 if _merge == 1
drop _merge

* Clean dataset
keep location_id year_id sex_id age_group_id intercept-re9

* Save the regression coefficients
export delimited using "FILEPATH", replace
