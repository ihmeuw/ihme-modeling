

** ***************************	
** Prep
** ***************************
	
	clear all 
	set more off
	pause on
	capture log close
	local test = 0
	
	if (c(os)=="Unix") {
		** global arg gets passed in from the shell script and is parsed to get the individual arguments below
		local jroot "FILEPATH"
		set odbcmgr unixodbc
		local code_dir "`1'"
		local ihme_loc_id "`2'"
		local ctemp "`3'"
		local location_id `4'
		
		if (`test' == 1) {
			local code_dir "FILEPATH"
			local ihme_loc_id "IND_43901"
			local ctemp = 1
		}
		
		if ("`ctemp'" == "1") {
			global root "FILEPATH"
		} 
		else {
			global root "FILEPATH"
		}
		local child_dir "FILEPATH"

		qui do "/home/j/Project/Mortality/shared/functions/get_locations.ado"
	} 
	if (c(os)=="Windows") { 
		global root "FILEPATH"
		local jroot "FILEPATH"
		local ihme_loc_id "GBR_44720"
		local child_dir "FILEPATH"
		qui do "FILEPATH"
	}
	
	
	
	di "$arg"
	di "`ihme_loc_id'"

	cd "FILEPATH"
	global pop_file "FILEPATH"
	global births_file "FILEPATH"
	

	insheet using "FILEPATH", clear

	keep if ihme_loc_id == "`ihme_loc_id'"
	drop if year > 2016.5 | year < 1949.5	
	rename med q5med
	rename lower q5lower
	rename upper q5upper
	keep ihme_loc_id year q5* 
	isid ihme_loc_id year
	tempfile estimates
	save `estimates', replace
	
** get sex-ratios at birth 
	use "$births_file", clear
	keep if ihme_loc_id == "`ihme_loc_id'"	
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

	insheet using "FILEPATH", clear 
	rename sim simulation
	rename mort q5_both
	drop if year > 2016.5 | year < 1949.5
	
	cap rename iso3 ihme_loc_id
	duplicates drop ihme_loc_id year simulation, force

	tempfile sims
	save `sims', replace 
	
** ***************************	
** Produce estimates from sex-model 
** ***************************
	
	merge m:1 ihme_loc_id simulation using "FILEPATH"
	drop if _m == 2
	drop _m 
	
	tostring q5_both, gen(merge_q5_both) format(%9.3f) force
	merge m:1 merge_q5_both simulation using "FILEPATH"
	drop if _m == 2
	drop _m merge_q5_both

	merge m:1 ihme_loc_id year using `sex_ratio'
	drop if _merge==2
	replace birth_sexratio=1.05 if _merge==1
	drop _merge 

	gen logit_q5_sexratio_pred = intercept + regbd + rebin
	gen q5_sexratio_pred = exp(logit_q5_sexratio_pred) / (1+exp(logit_q5_sexratio_pred))
	replace q5_sexratio_pred = (q5_sexratio_pred*0.7) + 0.8
	gen logit_q5_sexratio_wre = intercept + regbd + reiso + rebin 


	gen q5_female = (q5_both*(1+birth_sexratio))/(1+q5_sexratio_pred*birth_sexratio)
	gen q5_male = q5_female*q5_sexratio_pred
	assert q5_female > 0
	assert q5_male > 0

** formatting
	keep ihme_loc_id year q5_* logit_q5_sexratio_pred simulation birth_sexratio logit_q5_sexratio_wre
	renpfix q5_ q5
	order ihme_loc_id year simulation logit_q5_sexratio_pred q5* logit_q5_sexratio_wre
	sort ihme_loc_id year simulation
	isid ihme_loc_id year simulation
	rename q5* q_u5_* 
	drop birth_sexratio

	collapse (mean) *q*, by(year ihme_loc_id)
	
** save files for space-time
	export delimited "FILEPATH", replace
	
exit, clear 
