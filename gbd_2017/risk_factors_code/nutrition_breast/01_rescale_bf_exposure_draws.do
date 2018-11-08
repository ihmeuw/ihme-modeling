** Project: RF: Suboptimal Breastfeeding
** Purpose: Format Draws and Rescale Exclusive/Predominant/Partial

//housekeeping
clear all
cap restore, not
cap log close
set mem 500m
set more off
set maxvar 32000

// set relevant locals
local GPR 				"FILEPATH"
local output_folder	 	"FILEPATH"

***NON-EXCLUSIVE BREASTFEEDING***

**exclusive breastfeeding**
import delimited "FILEPATH", clear
drop v1
duplicates drop location_id year_id sex_id, force
forvalues n = 0/999 {
	rename draw_`n' exp_cat4_`n'
}

tempfile ebf
save `ebf', replace

**predominant breastfeeding**
import delimited "FILEPATH", clear
drop v1
duplicates drop location_id year_id sex_id, force
forvalues n = 0/999 {
	rename draw_`n' exp_cat3_`n'
}

tempfile predbf
save `predbf', replace

**partial breastfeeding**
import delimited "FILEPATH", clear
drop v1
duplicates drop location_id year_id sex_id, force
forvalues n = 0/999 {
	rename draw_`n' exp_cat2_`n'
}

tempfile partbf
save `partbf', replace

**any breastfeeding**
import delimited "FILEPATH", clear
drop v1
duplicates drop location_id year_id sex_id, force
forvalues n = 0/999 {
	rename draw_`n' abf_`n'
}

merge 1:1 location_id year_id sex_id using `ebf', keep(match) nogen
merge 1:1 location_id year_id sex_id using `predbf', keep(match) nogen
merge 1:1 location_id year_id sex_id using `partbf', keep(match) nogen

*** rescale draws
forvalues n = 0/999 {
	gen exp_cat1_`n' = 1-abf_`n'
	quietly gen scale_`n' = abf_`n'/(exp_cat2_`n'+exp_cat3_`n'+exp_cat4_`n')
	forvalues m = 2/4 {
		quietly replace exp_cat`m'_`n' = scale_`n'*exp_cat`m'_`n'
	}
}
drop abf* scale*
compress
save "FILEPATH", replace


**DISCONTINUED BREASTFEEDING**

**6 to 11 months**
import delimited "FILEPATH", clear
drop v1
duplicates drop location_id year_id sex_id, force
forvalues n = 0/999 {
	rename draw_`n' exp_cat2_`n'
	gen exp_cat1_`n' = 1 - exp_cat2_`n'
	order exp_cat1_`n' exp_cat2_`n', after(year)
}
save "FILEPATH", replace

**12 to 23 months**
import delimited "FILEPATH", clear
drop v1
duplicates drop location_id year_id sex_id, force
forvalues n = 0/999 {
	rename draw_`n' exp_cat2_`n'
	gen exp_cat1_`n' = 1 - exp_cat2_`n'
	order exp_cat1_`n' exp_cat2_`n', after(year)
}
save "FILEPATH", replace

