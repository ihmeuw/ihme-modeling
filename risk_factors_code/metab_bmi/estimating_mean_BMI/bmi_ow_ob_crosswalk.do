clear all
set more off

// System stuff
	if c(os) == "Unix" {
		local prefix "/home/j"
		set more off
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local prefix "J:"
	}

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
	
	expand 2
	bysort location_id: gen sex_id = 0 + _n
	expand 13
	bysort location_id sex_id: gen age_group_id = 8 + _n
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

drop if age_group_id < 9

mixed bmi_mean overweight_mean obese_mean i.age_start i.sex_id || super_region_name: overweight_mean obese_mean || region_name: overweight_mean obese_mean || ihme_loc_id: overweight_mean obese_mean, reml

* Extract the random effects
predict re*, reffects

mat b = e(b)'
mat v = e(V)
mat b2 = b[1..18, 1]
mat v2 = v[1..18, 1..18]

preserve
clear
set obs 1000
drawnorm ow_coeff ob_coeff age20 age25 age30 age35 age40 age45 age50 age55 age60 age65 age70 age75 age80 sex1 sex2 intercept, means(b2) cov(v2)
gen draw = _n -1
reshape long age, i(ow_coeff ob_coeff sex1 sex2 intercept draw) j(age_start)
reshape long sex, i(ow_coeff ob_coeff intercept age draw) j(sex_id)
replace sex = 0 if sex_id == 1
replace age = 0 if age_start == 20

reshape wide ow_coeff ob_coeff intercept age sex, i(age_start sex_id) j(draw)
foreach i of numlist 0/999 {
	gen int`i' = intercept`i' + age`i' + sex`i'
	drop intercept`i' age`i' sex`i'
}
rename int* intercept*

keep age_start sex_id ow_coeff* ob_coeff* intercept*
duplicates drop *, force
tempfile fixed_coeffs
save `fixed_coeffs', replace
restore

* Creat tempfiles of coefficients to merge to prediction frame
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
keep location_id sex_id age_group_id ow_coeff0-re9

expand 2 if age_group_id == 21, gen(exp)
replace age_group_id = 30 if exp == 1
drop exp
expand 2 if age_group_id == 21, gen(exp)
replace age_group_id = 31 if exp == 1
drop exp
expand 2 if age_group_id == 21, gen(exp)
replace age_group_id = 32 if exp == 1
drop exp
expand 2 if age_group_id == 21, gen(exp)
replace age_group_id = 235 if exp == 1
drop exp

drop if age_group_id == 21

* Save the regression coefficients
export delimited using "FILEPATH", replace
