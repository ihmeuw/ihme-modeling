// Purpose: Creates sensitivity and specificity adjustment matrix of draws 
// for use in the diarrhea etiology PAF calculations.
// Campylobacter used EIA in MALED, which was not a standard for the 
// detection of this pathogen in the scientific literature so we only use
// the GEMS results (bacterial culture). Norovirus is very different between
// GEMS and MALED, due to the type of PCR used (RT and qPCR). As it is 
// unclear how the preference is for detection in the scientific literature,
// for now we determined that it would be appropriate to take half the draws
// from MALED and half from GEMS. 

clear all
set more off


import delimited "filepath", clear
tempfile eti_ids 
save `eti_ids'


// Import values used in GBD 2016 for Campylobacter and Norovirus //
import delimited "filepath", clear
tempfile gbd16
save `gbd16'

keep if pathogen == "campylobacter" // | pathogen == "norovirus"
tempfile replace
save `replace'

use `gbd16', clear
keep if pathogen == "norovirus"
keep pathogen measure draw_1 - draw_500
tempfile noro
save `noro'

import delimited "filepath", clear
tempfile new
save `new'

keep if pathogen == "norovirus"
keep pathogen measure draw_501 - draw_1000
merge 1:1 pathogen measure using `noro', nogen
save `noro', replace

use `new', clear
drop if pathogen == "campylobacter" | pathogen == "norovirus"
append using `replace'
append using `noro'
gen regression_name = pathogen
drop mean - range
drop v1
tempfile master
save `master'

keep if measure=="sensitivity"
forval i = 1/1000{
	rename draw_`i' sensitivity_`i'
}
tempfile sensitivity
save `sensitivity'

use `master', clear
keep if measure=="specificity"
forval i = 1/1000 {
	rename draw_`i' specificity_`i'
}

merge 1:1 regression_name using `sensitivity', nogen

merge 1:1 regression_name using `eti_ids',
keep if _m==3
drop _m
drop measure
sort rei_name

egen mean_sen = rowmean(sensit*)
egen lower_sen = rowpctile(sensit*), p(2.5)
egen upper_sen = rowpctile(sensit*), p(97.5)

egen mean_spe = rowmean(specif*)
egen lower_spe = rowpctile(specif*), p(2.5)
egen upper_spe = rowpctile(specif*), p(97.5)

saveold "filepath", replace
export delimited "filepath", replace
