// Use DisMod input for age distribution and country-level paf estimates from
// 00_studylevel_hib.do for final hib paf estimates //
// This file doesn't have to be run for Hib but is required for
// pneumococcal paf estimation in 02_pneumo_final_paf.do"

// Set up //
if c(os)=="Unix" global j "/home/j"
else global j "J:"
set more off
cap log close
clear all
set matsize 8000
set maxvar 10000

local date = c(current_date)

do "FILEPATH/get_covariate_estimates.ado"
do "FILEPATH/get_location_metadata.ado"

// Get IHME location metadata //
get_location_metadata, location_set_id(9) clear
tempfile locations
save `locations'

// Get vaccine coverage estimates //
get_covariate_estimates, covariate_name_short(Hib3_coverage_prop) clear
drop age_group_id
drop sex_id
gen fcovmean = mean_value
gen fcovsd = (upper_value-lower_value)/2/1.96
gen hibmean = mean_value

tempfile hib_cov
save `hib_cov'
	
local vi .8 

local cnt 0

// Import age distribution from DisMod //
		insheet using "FILEPATH/hib_pred_out.csv", comma clear
		replace pred_median = pred_median[1] if age_upper < 5
		drop if age >5
		drop if _n == 1
		local scale = pred_median[1]
		replace pred_median = pred_median / `scale'
		gen mrg = 1
		
		tempfile agescalar
		save `agescalar'
		
		use "FILEPATH/hib_RCT_studies.dta", clear

// Meta analysis again of Hib RCT PAFs //
qui	    metan adlnrr adlnrrse `if', random title("`pat' Meta analysis of adjusted rr for `st'") nograph
		local par  `r(ES)' 
		local parl  `r(ci_low)'
		local paru  `r(ci_upp)'
		
		clear 
		set obs 1
		gen adlnrr = `par'
		gen adlnrrse = (`paru' - `parl') / (2*1.96)

		gen _0id = _n
		forval i = 1/1000 {
			gen draw`i' = rnormal(adlnrr, adlnrrse)
		}

		gen mrg = 1
		
// Use that value and adjust for age distribution //
		merge 1:m mrg using `agescalar', keepusing(age pred_median)

		qui foreach var of varlist draw1-draw1000 {
				replace `var' = `var' / pred_median
			}
				
		keep age adlnrr adlnrrse draw1-draw1000
		tostring age,force replace
		replace age = ".01" if age == ".0099999998"
		replace age = ".1" if age == ".1000000015"
		egen adve = rowmean(draw*)
		label variable adve "Adjusted VE for Hib"
		gen age_group_id = _n+1
		
		merge 1:1 age_group_id using "FILEPATH/age_mapping.dta", nogen
		keep if order!=.
		drop if age_group_id==235
	// Replace age groups over 5 with 0 for base PAFs //
		forval i = 1/1000{
			replace draw`i' = 0 if age_group_id>5
		}
		
		saveold "FILEPATH/hib_age_study_paf.dta", replace

		tempfile madve
		save `madve'
				
		levelsof age, local(ages) c

// Return to Hib vaccine coverage, prep for uncertainty by taking 1000 draws //
		use location_id year_id fcovmean fcovsd using `hib_cov', clear
		drop if fcovmean == .

		// This takes a long time //
		run "FILEPATH\gen draws for a file.do" fcovmean fcovsd
		gen mrg = 1
		saveold "FILEPATH/hib_fcov_draws.dta", replace
		tempfile main_i
		save `main_i'

// Get 1000 draws for optimal VE //		
		clear 
		set obs 1
		forval i = 1/1000 {
			gen optv`i' = `vi'
		}
		gen mrg = 1
		tempfile optve
		save `optve'
		
// Everything is prepped! //
// Part 1 //
	use `madve' if age == "1", replace
	gen mrg = 1
	cap erase `madveuse'
	tempfile madveuse
	save `madveuse'
	
	use `main_i', clear
	
	merge m:1 mrg using `optve', nogen

	merge m:1 mrg using `madveuse', nogen

	keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2016)
			
// PAF estimation //
qui forval i =1/1000 {
		replace v`i' = 1 if v`i' > 1
		replace v`i' = 0 if v`i' < 0
		replace draw`i' = draw`i' * (1 - v`i' * optv`i') / (1 - draw`i' * v`i' * optv`i')
	}
	egen paf_mean = rowmean(draw*)
	egen paf_sd = rowsd(draw*)
	egen float paf_lower = rowpctile(draw*), p(2.5)
	egen float paf_upper = rowpctile(draw*), p(97.5)

cap drop optv1-optv1000
cap drop v1-v1000
save "FILEPATH/hib_paf_draws_2.dta", replace

