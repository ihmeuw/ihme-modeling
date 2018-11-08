** DATE: 20 April 2011
** OUTLINE OF CODE:
** 	Prep
** 		- load GPR estimates and sims
** 	Get sex-specific estimates
** 		- merge on sex model coefficients
** 		- calculate sex-ratio
** 		- calculate male and female 5q0 for each simulation
**

** ***************************
** Prep
** ***************************

	clear all
	set more off
	pause on
	capture log close
	local test = 0

	local 5q0_dir "FILEPATH"
	local 5q0_gpr_simulations_file "FILEPATH"
	local 5q0_gpr_mean_file "FILEPATH"


	log using "FILEPATH", replace


	global births_file "FILEPATH"

** save GPR 5q0 estimates for later use
	insheet using "FILEPATH", clear

	keep if location_id == `location_id' & estimate_stage_id == 3
	gen ihme_loc_id = "`ihme_loc_id'"
	keep if year_id >= `start_year' & year_id <= `end_year'
	rename mean q5med
	rename lower q5lower
	rename upper q5upper
	rename viz_year year
	keep ihme_loc_id year q5*
	isid ihme_loc_id year
	tempfile estimates
	save `estimates', replace

** get sex-ratios at birth
	insheet using "$births_file", clear
	keep if location_id == `location_id'
	gen ihme_loc_id = "`ihme_loc_id'"
	keep ihme_loc_id year sex births
	reshape wide births, i(ihme_loc_id year) j(sex, string)
	gen birth_sexratio = birthsmale/birthsfemale
	keep ihme_loc_id year birth_sexratio
	replace year = year + 0.5
	replace birth_sexratio = 1.05 if birth_sexratio == .
	tempfile sex_ratio
	save `sex_ratio', replace

** ***************************
** Load sims from GPR
** ***************************
	insheet using "`5q0_gpr_simulations_file'", clear
	rename sim simulation
	rename mort q5_both
	keep if year >= `start_year' + 0.5 & year <= `end_year' + 0.5

	tempfile sims
	save `sims', replace

** ***************************
** Produce estimates from sex-model
** ***************************

** merge simulated 5q0(both)'s with the sex model parameters
	merge m:1 ihme_loc_id simulation using "FILEPATH"
	drop if _m == 2
	drop _m

	tostring q5_both, gen(merge_q5_both) format(%9.3f) force
	merge m:1 merge_q5_both simulation using "FILEPATH"
	drop if _m == 2
	drop _m merge_q5_both

** merge in birth sex-ratios
	merge m:1 ihme_loc_id year using `sex_ratio'
	drop if _merge==2
	replace birth_sexratio=1.05 if _merge==1
	drop _merge

** calculate predicted sex-ratio
	gen logit_q5_sexratio_pred = intercept + regbd + rebin
	gen q5_sexratio_pred = exp(logit_q5_sexratio_pred) / (1+exp(logit_q5_sexratio_pred))
	replace q5_sexratio_pred = (q5_sexratio_pred*0.7) + 0.8
	gen logit_q5_sexratio_wre = intercept + regbd + reiso + rebin // 8/18/16 calculating with location RE in order to calculate MAD for GPR

** generate predicted values using sex ratio at birth
	gen q5_female = (q5_both*(1+birth_sexratio))/(1+q5_sexratio_pred*birth_sexratio)
	gen q5_male = q5_female*q5_sexratio_pred
	assert q5_female > 0
	assert q5_male > 0

** formatting
	keep ihme_loc_id year q5_* logit_q5_sexratio_pred simulation birth_sexratio logit_q5_sexratio_wre
	renpfix q5_ q5
	** reshape long q5, i(ihme_loc_id year simulation) j(sex, string)
	order ihme_loc_id year simulation logit_q5_sexratio_pred q5* logit_q5_sexratio_wre
	sort ihme_loc_id year simulation
	isid ihme_loc_id year simulation
	rename q5* q_u5_*
	drop birth_sexratio

** collapsing to mean level for space-time
	collapse (mean) *q*, by(year ihme_loc_id)

** save files for space-time
	export delimited "FILEPATH", replace

exit, clear
