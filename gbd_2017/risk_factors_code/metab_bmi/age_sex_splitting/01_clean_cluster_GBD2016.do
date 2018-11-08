clear all
set more off
set varabbrev off
*set maxvar 20000
set trace on

if c(os) == "Unix" {
	local prefix "/home/j"
	set more off
	set odbcmgr unixodbc
}
else if c(os) == "Windows" {
	local prefix "J:"
}

local iso3 `1'
local fold `2'
local date `3'

import delimited using "FILEPATH", clear varnames(1)
tempfile age_categorical_map
save `age_categorical_map', replace
insheet using "FILEPATH", clear																			

tempfile months_cutoffs																			
save `months_cutoffs', replace																			
insheet using "FILEPATH", clear	
tempfile years_cutoffs																			
save `years_cutoffs', replace										

local files: dir "`fold'/`iso3'" files "*.dta", respectcase
foreach f in `files' {
	use "`fold'/`iso3'/`f'", clear

    cap confirm string variable age_categorical
	if !_rc {
		cap confirm numeric variable age_year
		if _rc {
			replace age_categorical = trim(age_categorical)
			merge m:1 age_categorical using `age_categorical_map', nogen keep(3)
			drop age_categorical
		}
	}

	foreach var in psu strata pweight bmi bmi_rep overweight overweight_rep obese obese_rep {
		cap confirm numeric variable `var'
		if _rc {
			gen `var' = .
		}
	}

	cap confirm numeric variable age_month
	if !_rc {
		replace bmi = . if age_month < 24
		replace bmi_rep = . if age_month < 24
		replace bmi = . if (bmi > 70 | bmi <10)
		replace bmi_rep = . if (bmi_rep > 70 | bmi_rep<10)
	}

	cap confirm numeric variable age_year
	if !_rc {
		replace bmi = . if age_year < 2
		replace bmi_rep = . if age_year < 2
		replace bmi = . if (bmi > 70 | bmi <10)
		replace bmi_rep = . if (bmi_rep > 70 | bmi_rep<10)
	}

	drop overweight overweight_rep obese obese_rep

	cap confirm numeric variable age_month
	if !_rc {
		merge m:1 age_month sex_id using `months_cutoffs'
		gen overweight = 1 if bmi >= ow_months & bmi != . & _merge == 3
		gen overweight_rep = 1 if bmi_rep >= ow_months & bmi_rep != . & _merge == 3
		gen obese = 1 if bmi >= ob_months & bmi != . & _merge == 3
		gen obese_rep = 1 if bmi_rep >= ob_months & bmi_rep != . & _merge == 3
		drop _merge ow_months ob_months
	}

	cap confirm numeric variable age_month
	if _rc {
		cap confirm numeric variable age_year
		if !_rc {
			replace age_year = round(age_year)
			merge m:1 age_year sex_id using `years_cutoffs', nogen keep(3)
			gen overweight = 1 if bmi >= ow & bmi != .
			gen overweight_rep = 1 if bmi_rep >= ow & bmi_rep != .
			gen obese = 1 if bmi >= ob & bmi != .
			gen obese_rep = 1 if bmi_rep >= ob & bmi_rep != .
			drop ow ob
		}
	}

	foreach var in overweight obese {
		replace `var' = 0 if `var' != 1 & bmi != .
		replace `var'_rep = 0 if `var'_rep != 1 & bmi_rep != .
	}

	foreach var of varlist bmi bmi_rep overweight overweight_rep obese obese_rep {
		count if `var' != .
			if `r(N)' == 0 {
			drop `var'
		}
	}


	cap mkdir "FILEPATH"
	export delimited using "FILEPATH", replace
}

