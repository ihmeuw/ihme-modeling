// Description: calculate MDR proportions weighted for the proportions of new and previously treated cases

// Definitions
// c_newinc: Total of new and relapse cases and cases with unknown previous TB treatment history
// ret_nrel: Previously treated patients, excluding relapse cases (pulmonary or extrapulmonary, bacteriologically confirmed or clinically diagnosed) **[available starting from 2013)
// ret_rel: Relapse cases
// ret_taf: Treatment after failure cases
// ret_tad:	Treatment after default cases
// ret_oth:	Other re-treatment cases
// ret_rel_labconf:	Relapse  pulmonary bacteriologically confirmed TB cases (smear positive or culture positive or positive by WHO-recommended rapid diagnostics such as Xpert MTB/RIF) **[available starting from 2013)
// ret_rel_clindx:	Relapse pulmonary clinically diagnosed TB cases (not bacteriologically confirmed as positive for TB, but diagnosed with active TB by a clinician or another medical practitioner who has decided to give the patient a full course of TB treatment) **[available starting from 2013)
// ret_rel_ep: Relapse extrapulmonary cases (bacteriologically confirmed or clinically diagnosed) **[available starting from 2013)

***********************************************************************************************************

clear all
set more off

// read in the proportions of relapse+retreated cases among all TB cases

use "FILEPATH\rel_ret_prop_draws.dta", clear

forvalues i=0/999 {
		replace draw_`i'=0.99 if draw_`i'>0.99
        }
		
forvalues i=0/999 {
		rename draw_`i' ret_prop_`i'
        }

tempfile ret_prop
save `ret_prop', replace


	forvalues i=0/999 {
		gen new_prop_`i'=1-ret_prop_`i'
        }
drop ret_prop_*

tempfile new_prop
save `new_prop', replace

********* Compute weighted average *********************************************

// bring in mdr_new draws

use "FILEPATH\mdr_new_draws.dta", clear

// use national proportions for UKR subnationals

preserve
keep if location_id==63
replace location_id=44934
tempfile tmp_1
save `tmp_1', replace
replace location_id=44939
tempfile tmp_2
save `tmp_2', replace
replace location_id=50559
append using `tmp_1'
append using `tmp_2'
tempfile ukr_sub
save `ukr_sub', replace
restore

drop if inlist(location_id, 44934, 44939, 50559)
append using `ukr_sub'


drop if inlist(location_id, 354, 361)

append using "FILEPATH\mdr_new_draws_354_361.dta"

forvalues i=0/999 {
		replace draw_`i'=0.99 if draw_`i'>0.99
        }

// merge on proportions of new TB cases
merge 1:1 location_id year_id using `new_prop', keep(3) nogen

forvalues i=0/999 {
		gen mdr_prop_wt_`i'=draw_`i'*new_prop_`i'
        }

drop new_prop_* draw_*

tempfile new_wt
save `new_wt', replace

// read in mdr_ret draws

use "FILEPATH\mdr_ret_draws.dta", clear

// use national proportions for UKR subnationals

preserve
keep if location_id==63
replace location_id=44934
tempfile tmp_1
save `tmp_1', replace
replace location_id=44939
tempfile tmp_2
save `tmp_2', replace
replace location_id=50559
append using `tmp_1'
append using `tmp_2'
tempfile ukr_sub
save `ukr_sub', replace
restore

drop if inlist(location_id, 44934, 44939, 50559)
append using `ukr_sub'

drop if inlist(location_id, 184, 197, 354, 361)

append using "FILEPATH\mdr_ret_draws_354_361_197_184.dta"

forvalues i=0/999 {
		replace draw_`i'=0.99 if draw_`i'>0.99
        }

// merge on proportions of retreated TB cases
merge 1:1 location_id year_id using `ret_prop', keep(3) nogen

forvalues i=0/999 {
		gen mdr_prop_wt_`i'=draw_`i'*ret_prop_`i'
        }

drop ret_prop_* draw_*

append using `new_wt'

collapse (sum) mdr_prop_wt_*, by(location_id year_id age_group_id sex_id) fast

forvalues i=0/999 {
		rename mdr_prop_wt_`i' draw_`i'
        }
		
save "FILEPATH\mdr_draws.dta", replace
