clear all
set more off
cap log close 

// System stuff
	if c(os) == "Unix" {
		local prefix "FILEPATH"
		set more off
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local prefix "FILEPATH"
	}

	adopath + "FILEPATH"

local date = subinstr("`c(current_date)'", " ", "", .)

get_location_metadata, location_set_id(22) clear
keep ihme_loc_id region_name super_region_name location_id
tempfile geo_hierarchy
save `geo_hierarchy', replace

// Log this script?
local log = 1
if `log' == 1 log using "FILEPATH", s replace

////////////////////////////////////////
//// Running the Regression for adjustment
////////////////////////////////////////

foreach indic in bmi_mean overweight_mean obese_mean {

		if "`indic'" == "overweight_mean" {
			local shortname "ow"
		}
		if "`indic'" == "obese_mean" {
			local shortname "ob"
		}
		if "`indic'" == "bmi_mean" {
			local shortname "bmi"
		}

	foreach sex of numlist 1 2 {

		* Read in the dataset
		use "FILEPATH", clear

		merge m:1 ihme_loc_id using `geo_hierarchy', nogen keep(3)

		* Generate the measured indicator
		gen measured = 1 if cv_diagnostic == "measured"
		replace measured = 0 if measured == .

		* Generate time period indicator (splitting into 10 yr periods, with a remainder)
		gen year_id = floor((year_start + year_end) / 2)
		gen time_period = "1980-1989" if inrange(year_id, 1980, 1989)
		replace time_period = "1990-1999" if inrange(year_id, 1990, 1999)
		replace time_period = "2000-2009" if inrange(year_id, 2000, 2009)
		replace time_period = "2010-2016" if inrange(year_id, 2010, 2017)

		* Running the self-report adjustment only for adults
		keep if age_start >= 15

		* Prep for log transform
		if "`indic'" == "bmi_mean" {
			gen log_bmi = log(bmi_mean)
		}
		
		* Prep for logit transform
		if "`indic'" == "overweight_mean" | "`indic'" == "obese_mean" {
			replace `indic' = .999 if `indic' == 1
			replace `indic' = .001 if `indic' == 0
			gen logit_`indic' = logit(`indic')
		}

		preserve

		keep if sex_id == `sex'
		
		if sex_id == 1 {
			local sexname "Male"
		}
		
		if sex_id == 2 {
			local sexname "Female"
		}

		* Test log space
		if "`indic'" == "bmi_mean" {
			mixed log_bmi measured##i.age_start || super_region_name: measured || region_name: measured || ihme_loc_id: || time_period:
		}
		
		* Test logit space
		if "`indic'" == "overweight_mean" | "`indic'" == "obese_mean" {
			mixed logit_`indic' measured##i.age_start || super_region_name: measured || region_name: measured || ihme_loc_id: || time_period:
		}

		* Extract random effect coefficients
		predict re*, reffects
		predict re_se*, reses

		* Extract fixed effect coefficients
		mat coeffs = e(b)
		mat variances = e(V)

		gen base_adjust = coeffs[1,2]
		gen base_variance = variances[2,2]

		gen age_effect = .
		local counter = 31
		foreach num of numlist 15(5)80 {
			replace age_effect = coeffs[1,`counter'] if age_start == `num'
			local counter = `counter' + 1
		}

		gen age_variance = .
		local counter = 31
		foreach num of numlist 15(5)80 {
			replace age_variance = variances[`counter',`counter'] if age_start == `num'
			local counter = `counter' + 1
		}

		* Generate adjustment factor (includes super region, and region random effects; take time period and country as noise)
		gen total_adjust = re1 + re3 + base_adjust + age_effect

		* Generate adjustments with random intercepts
		if "`indic'" == "bmi_mean" {
			gen adjusted = log_bmi + total_adjust if measured == 0
			replace adjusted = log_bmi if measured == 1	
		}
		
		if "`indic'" == "overweight_mean" | "`indic'" == "obese_mean" {
			gen adjusted = logit_`indic' + total_adjust if measured == 0
			replace adjusted = logit_`indic' if measured == 1
		}
	
		if "`indic'" == "bmi_mean" {
			replace adjusted = exp(adjusted)
		}
	
		if "`indic'" == "overweight_mean" | "`indic'" == "obese_mean" {
			replace adjusted = invlogit(adjusted)
		}

		tempfile `indic'_sex_`sex'
		save ``indic'_sex_`sex'', replace

		restore
		
	}
}

////////////////////////////////////////
//// Adjustment: Prepping and running it
////////////////////////////////////////

foreach indic in bmi_mean overweight_mean obese_mean {

		if "`indic'" == "overweight_mean" {
			local shortname "ow"
			local longname "overweight"
		}
		if "`indic'" == "obese_mean" {
			local shortname "ob"
			local longname "obese"
		}
		if "`indic'" == "bmi_mean" {
			local shortname "bmi"
			local longname "bmi"
		}

	use ``indic'_sex_1', clear
	append using ``indic'_sex_2'
	keep age_start sex_id age_effect super_region_name re1 region_name re3 re_se1 re_se3 base_adjust base_variance age_variance

	* Grab base adjust
	preserve
		keep sex_id base_adjust base_variance
		rename base_adjust base_adjust
		rename base_variance base_variance
		duplicates drop *, force
		tempfile base_adjust
		save `base_adjust', replace
		collapse (mean) base_adjust base_variance
		gen sex_id = 3
		append using `base_adjust'
		save `base_adjust', replace
	restore

	* Grab age effects
	preserve
		keep age_start sex_id age_effect age_variance
		rename age_effect age_effect
		rename age_variance age_variance
		duplicates drop *, force
		tempfile age_effects
		save `age_effects', replace
		collapse (mean) age_effect age_variance, by(age_start)
		gen sex_id = 3
		append using `age_effects'
		save `age_effects', replace
	restore

	* Grab geographic effects
	preserve
		keep super_region_name re1 re_se1 sex_id
		rename re1 re1
		rename re_se1 re_se1
		duplicates drop *, force
		tempfile super_region_effects
		save `super_region_effects', replace
		collapse (mean) re1 re_se1, by(super_region_name)
		gen sex_id = 3
		append using `super_region_effects'
		save `super_region_effects', replace
	restore
	preserve
		keep region_name re3 re_se3 sex_id
		rename re3 re3
		rename re_se3 re_se3
		duplicates drop *, force
		tempfile region_effects 
		save `region_effects', replace
		collapse (mean) re3 re_se3, by(region_name)
		gen sex_id = 3
		append using `region_effects'
		save `region_effects', replace
	restore

	////////////////////////////
	//// Run the Adjustment
	////////////////////////////
	use "FILEPATH", clear

	merge m:1 ihme_loc_id using `geo_hierarchy', nogen keep(3)

	* Generate the measured indicator
	gen measured = 1 if cv_diagnostic == "measured"
	replace measured = 0 if measured == .

	* Generate time period indicator (splitting into 10 yr periods, with a remainder)
	gen year_id = floor((year_start + year_end) / 2)
	gen time_period = "1980-1989" if inrange(year_id, 1980, 1989)
	replace time_period = "1990-1999" if inrange(year_id, 1990, 1999)
	replace time_period = "2000-2009" if inrange(year_id, 2000, 2009)
	replace time_period = "2010-2016" if inrange(year_id, 2010, 2017)
	
	* Generate the mid age, which is rounded to the closest 5 year age group
	gen age_mid = round(((age_start + age_end)/2), 5)
	gen age_temp = age_start // Store the actual age_start while we merge
	drop age_start
	rename age_mid age_start

	replace age_start = 80 if age_start > 80 & age_start != .

	keep if age_start >= 15

	merge m:1 sex_id using `base_adjust', nogen
		**erase `base_adjust'
	merge m:1 age_start sex_id using `age_effects', nogen
		**erase `age_effects'
	merge m:1 super_region_name sex_id using `super_region_effects', nogen
		**erase `super_region_effects'
	merge m:1 region_name sex_id using `region_effects', nogen
		**erase `region_effects'

	bysort region_name: egen temp = mean(re3)
	replace re3 = temp if re3 == .
	drop temp
	bysort region_name: egen temp = mean(re_se3)
	replace re_se3 = temp if re_se3 == .
	drop temp
	bysort super_region_name: egen temp = mean(re1)
	replace re1 = temp if re1 == .
	drop temp
	bysort super_region_name: egen temp = mean(re_se1)
	replace re_se1 = temp if re_se1 == .
	drop temp

	* Prep for log transform
	if "`indic'" == "bmi_mean" {
		gen log_bmi = log(bmi_mean)
		gen log_variance = (bmi_se)^2 * (1/bmi_mean)^2
	
		* Adjust mean
		gen log_adjusted_mean = log_bmi + base_adjust + age_effect + re1 + re3 if cv_diagnostic == "self-report"
		replace log_adjusted_mean = log_bmi if cv_diagnostic == "measured"

		* Adjust variance
		gen log_adjusted_variance = log_variance + base_variance + age_variance + re_se1^2 + re_se3^2 if cv_diagnostic == "self-report"
		replace log_adjusted_variance = log_variance if cv_diagnostic == "measured"

		* Back transform to normal space
		gen adjusted_mean = exp(log_adjusted_mean)
		gen adjusted_variance = log_adjusted_variance / (1/adjusted_mean)^2
	}
	
	if "`indic'" == "overweight_mean" | "`indic'" == "obese_mean" {
		replace `indic' = .999 if `indic' == 1
		replace `indic' = .001 if `indic' == 0
		gen logit_mean = logit(`indic')
		gen logit_variance = (`longname'_se)^2 * (1/(`indic'*(1-`indic')))^2
	
		* Adjust mean
		gen logit_adjusted_mean = logit_mean + base_adjust + age_effect + re1 + re3 if cv_diagnostic == "self-report"
		replace logit_adjusted_mean = logit_mean if cv_diagnostic == "measured"

		* Adjust variance
		gen logit_adjusted_variance = logit_variance + base_variance + age_variance + re_se1^2 + re_se3^2 if cv_diagnostic == "self-report"
		replace logit_adjusted_variance = logit_variance if cv_diagnostic == "measured"

		* Back transform to normal space
		gen adjusted_mean = invlogit(logit_adjusted_mean)
		gen adjusted_variance = logit_adjusted_variance / (1/(adjusted_mean * (1-adjusted_mean)))^2
	}
	

	* Replace with true age start
	replace age_start = age_temp

	* Clean dataset
	keep nid year_start year_end ihme_loc_id sex_id cv_urbanicity cv_diagnostic age_start age_end smaller_site_unit year_id adjusted_mean adjusted_variance location_id `longname'_ss `longname'_sd
	rename `longname'_sd standard_deviation
	rename adjusted_variance variance
	rename adjusted_mean data
	rename `longname'_ss sample_size

	export delimited using "FILEPATH", replace
}


////////////////////////////////////////
//// Children under 15
////////////////////////////////////////
foreach indic in bmi_mean overweight_mean obese_mean {

		if "`indic'" == "overweight_mean" {
			local shortname "ow"
			local longname "overweight"
		}
		if "`indic'" == "obese_mean" {
			local shortname "ob"
			local longname "obese"
		}
		if "`indic'" == "bmi_mean" {
			local shortname "bmi"
			local longname "bmi"
		}

		* Read in the dataset
		use "FILEPATH", clear

		gen year_id = floor((year_start + year_end) / 2)

		merge m:1 ihme_loc_id using `geo_hierarchy', nogen keep(3)

		* Generate the measured indicator
		gen measured = 1 if cv_diagnostic == "measured"
		replace measured = 0 if measured == .

		* Running the self-report adjustment only for adults
		keep if age_start < 15
		
		drop if measured == 0

		rename `longname'_mean data
		gen variance = `longname'_se^2
		rename `longname'_sd standard_deviation
		rename `longname'_ss sample_size

		keep nid year_start year_end ihme_loc_id sex_id cv_urbanicity cv_diagnostic sample_size standard_deviation age_end smaller_site_unit location_id year_id age_start data variance
		
		export delimited using "FILEPATH", replace
}
	

cap log close

