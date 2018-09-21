****************************
**** 	1. Set Up		****
****************************

clear all
set more off
set maxvar 20000


	if c(os) == "Unix" {
		local prefix "FILEPATH"
		set more off
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local prefix "FILEPATH"
	}

// Locals
	// Taking Parameters (location_id)
		local loc_id `1'
		
// Read in the regression coefficients dataset
import delimited using "FILEPATH", clear
tempfile coefficients
save `coefficients', replace

// Read in the overweight dataset
import delimited using "FILEPATH", clear

	**rename gpr_mean overweight_mean
	forvalues x=0/999 {
		rename draw_`x' overweight_`x'
	}

	tempfile overweight
	save `overweight', replace

// Read in obesity dataset
import delimited using "FILEPATH", clear

	forvalues x=0/999 {
		rename draw_`x' obesity_`x'
	}

// Combine draw files for overweight and obesity
merge 1:1 location_id year_id age_group_id sex_id using `overweight', nogen keep(3)
merge m:1 location_id age_group_id sex_id using `coefficients', nogen keep(3)

* Run prediction
forvalues x=0/999 {
gen draw_`x' = intercept`x' + (overweight_`x'*ow_coeff`x') + (obesity_`x'*ob_coeff`x') + (re1*overweight_`x') + (re2*obesity_`x') + re3 + (re4*overweight_`x') + (re5*obesity_`x') + re6 + (re7*overweight_`x') + (re8*obesity_`x') + re9
replace draw_`x' = exp(draw_`x')
}

keep location_id year_id sex_id age_group_id draw_*
gen measure_id = 19

**save a separate dataset that are just the means for visualization
preserve
egen mean_draw = rowmean(draw_*)
egen mean_upper = rowpctile(draw_*), p(97.5)
egen mean_lower = rowpctile(draw_*), p(2.5)
egen standard_error = rowsd(draw_*)
drop draw_*
export delimited "FILEPATH", replace
restore

* Save draw files
cap mkdir "FILEPATH"
export delimited using "FILEPATH", replace
