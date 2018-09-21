// Calculate study-level attributable fractions for Hib 
// These outputs are then used in DisMod to calculate age-specific PAFs
// the meta-analytic effect size result is then adjusted for national-level
// values to estimate a final hib paf.
// Hib must be run before pneumococcus paf. 
// Set up files and functions //

clear all
set more off
cap log close
set maxvar 15000
set seed 2038947

do "FILEPATH/get_covariate_estimates.ado"
do "FILEPATH/get_best_model_versions.ado"
do "FILEPATH/get_location_metadata.ado"

// run Hib first
local pat  hib
local file Study-Level_PAFs

// This is an adjustment to account for imperfect vaccine effectiveness
local vi .8   // This is from a systematic review

// Get vaccine coverage estimates //
get_covariate_estimates, covariate_name_short(Hib3_coverage_prop) clear
drop age_group_id
gen fcovmean = mean_value
gen fcovsd = (upper_value-lower_value)/2/1.96
tempfile hib_cov
save `hib_cov'

get_covariate_estimates, covariate_name_short(PCV3_coverage_prop) clear
drop age_group_id
gen pcv_mean = mean_value
gen pcv_sd = (upper_value-lower_value)/2/1.96
tempfile pcv_cov
save `pcv_cov'

// Get IHME location metadata
get_location_metadata, location_set_id(9) clear
tempfile locations
save `locations'

// Right now Hib and PCV are done separately but Hib must be done first //

*****Reads the data and generates new varaibles: ve as vaccine efficacy, vi as vaccine efficacy against invasive, and cov as coverage"*******
// Extracted study data, format variables in RR and log space
		import delimited "FILEPATH/hib-data.csv", clear		
		drop if status == "EXCLUDE"
		gen author = first + "_" +  iso3 + string(studyyear_end)
		gen ihme_loc_id = iso3
		gen rr_inv = (100 - veinvasivedisease) / 100
		gen rr_inv_low = (100 - upper_invasive ) / 100
		gen rr_inv_up = (100 - lower_invasive ) / 100
		
		foreach var of varlist rr_* {
					gen ln_`var' = ln(`var')
		}
		
		gen ln_rr_se = (ln_rr_inv_up- ln_rr_inv) / 1.96
	keep if studytype=="RCT"

	// convert to log space //
	encode studytype,gen(study_type) 
	gen lnrr = ln(( 100 - ve)/100)
	gen lnrrse = (lnrr - ln(( 100 - veu)/100))/1.96
	foreach p in ve vi{
		gen lnrr_`p'se = (lnrr_`p'u - lnrr_`p'l)/(2*1.96)
	}

forval i = 1/1000 {
	gen draw`i' = rnormal(ve, ve_se) / rnormal(vi, vi_se) / rnormal(cov, covse)
}
	egen adlnrr = rowmean(draw*)
	egen adlnrrse = rowsd(draw*)
	

//// Create a non-age specific PAF for country, year after adjusting for country level Hib vaccine coverage ////
// This will be used in the pneumococcal PAF calculation //
// Meta-analysis of the study-level PAFs //
	metan adlnrr adlnrrse, random nograph
	local par  `r(ES)' 
	di in red `par' 
	local parl  `r(ci_low)'
	local paru  `r(ci_upp)'

// Pulls back in the Hib vaccine coverage covariate //	
	use location_id year_id fcovmean fcovsd using `hib_cov', clear
	gen adve = (`par')
	gen advese = adve * (`paru' - `parl') / (2*1.96)
	gen optvi = `vi'
	
	label variable adve "Crude PAF for Hib"
	label variable advese "Standard error of the crude PAF for Hib"
	label variable optvi "Current country Hib vaccination effectiveness (`vi')"

// This takes a long time because it has to sample 1000 draws from 20,000 obs! //
// but is necessary for uncertainty //
	drop if year == .
	run "FILEPATH\gen draws for a file.do" fcovmean fcovsd
	gen mrg = 1
	tempfile main_i
	save `main_i'
	
// The base study-level PAF, 'adve' is just one value (effect size from meta-analysis of RCTs) //
	clear 
	set obs 1
	gen adve = (`par')
	gen advese = adve * (`paru' - `parl') / (2*1.96)
	gen _0id = _n
	run "FILEPATH/gen matrix of draws.do" adve advese madve
	svmat madve, names(ad) 
	gen mrg = 1
	tempfile madve
	save `madve'
	
// Merge them together, calculate PAF by matrix multiplication ! //
	use `main_i', clear
	merge m:1 mrg using `madve'
	
	forval i =1/1000 {
		gen paf`i' = ad`i' * (1 - v`i' * optvi) / (1 - ad`i' * v`i' * optvi)
	}
	
	egen paf_mean = rowmean(paf*)
	egen paf_sd = rowsd(paf*)
	gen finve = paf_mean
	drop v* ad* id var2 mrg _0id _m

	save "FILEPATH/hib_RCT_studypaf.dta", replace		
