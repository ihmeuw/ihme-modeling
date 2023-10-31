* Purpose: Apply assumptions to estimates
* Notes: Pull the par adjusted base model and split to species based on grs and coinfection splits

*** ======================= BOILERPLATE ======================= ***
	clear all
	set more off
	set maxvar 32767
	local user : env USER
	local code_root "FILEPATH"
    local data_root "FILEPATH"

	local params_dir "FILEPATH/params"
	local draws_dir "FILEPATH/draws"
	local interms_dir "FILEPATH/interms"
	local logs_dir "FILEPATH/logs_dir"
	local location_id			`10'

	cap log using "`logs_dir'/step_6_`location'.smcl", replace
	if !_rc local close_log 1
	else local close_log 0

	// Source relevant functions
	adopath + FILEPATH  // source STATA
    local gbd_round_id "ADDRESS"
    local decomp_step "ADDRESS"

    
capture mkdir "`interms_dir'/ADDRESS_prevalence"

tempfile  raw appendTemp mergeTemp  uhc
use "ADDRESS/uhc.dta", clear
keep if location_id==`location'
save `uhc', replace
	
use "ADDRESS/incidence_draws/`location'.dta", clear
	 
drop if year_id>2018 | year_id<1989
	 
	 *merge hsa file with incidence file
	 merge m:1 year_id using `uhc', assert(3) nogenerate
	
	*** ESTIMATE PREVALENCE FROM INCIDENCE 
	drop run_id v1 metric_id
	
	*generate mean value of covariate for the location
		bysort year_id: egen uhcMean = mean(mean_value)
		drop mean_value ageCurve* totalPop 
		
		
		*drop if  inlist(age_group_id,238,389)
		* We'll need the raw incidence draws to estimate acute prevalence later; save them to a tempfile here *
		tempfile raw
		save `raw', replace 
		
			
				
		rename draw_* draw_*__
		drop population 
		sort year_id age_start sex_id
		*drop years due to memory constraints (maxvar)
		
				
*drop population variable here, switch to age_start instead of age_group_id
		reshape wide draw_*__  uhcMean, i(age_start location_id sex_id model_idmeasure_id) j(year_id)

		
		* Calculate the duration spent in a given age category *
		generate duration = age_end - age_start
		replace duration=1 if age_group_id==388
		replace duration=1 if age_group_id==389
		replace duration=1 if age_group_id==238
		replace duration=2 if age_group_id==34


		
		* Loop through draws and years, and move cases through cohort space to estimate prevalence of chronic sequelae
		forvalues i = 0/999 {
			forvalues y = 1990/2000 {
				quietly {
					generate inc_long_`i'__`y' = draw_`i'__`y' * (1 - uhcMean`y') * 0.476 // Assume that uhcMean equals the proportion who will recieve timely tx and not have chronic sequelae; 0.476 is proportion of those not receiving tx who will have visible scars (from GBD meta-analysis)
					local order `order' draw_`i'__`y' inc_long_`i'__`y'
					
					generate prev_`i'__`y' = 0 if inlist(age_group_id,2,3,388) 
					
					
					if `y'== 1990 {
						bysort sex_id (age_start): replace prev_`i'__`y' = prev_`i'__`y'[_n-1] + (1 - prev_`i'__`y'[_n-1]) * (1 - exp(-(age_end[_n-1] - age_start[_n-1]) * inc_long_`i'__`y'[_n-1])) if age_start>=.5
						* Prevalence of longterm sequelae in each age category (half-year correction)
						replace prev_`i'__`y' = prev_`i'__`y' + (1 - prev_`i'__`y') * (1 - exp(-(age_end - age_start)/2 * inc_long_`i'__`y')) if (age_start < 95 & age_start>=.5)
						replace prev_`i'__`y' = prev_`i'__`y' + (1 - prev_`i'__`y') * (1 - exp(-(100 - age_start)/2 * inc_long_`i'__`y')) if age_start ==  95
						*replace prev_`i'__`y' = 0 if age_start<.5
						}
					else {
						bysort sex_id (age_start): replace prev_`i'__`y' = prev_`i'__`y'[_n-1] + (1 - prev_`i'__`y'[_n-1]) * (1 - exp(-(age_end[_n-1] - age_start[_n-1]) * inc_long_`i'__`y'[_n-1])) if inlist(age_group_id,389,238,34)
						bysort sex_id (age_start): replace prev_`i'__`y' = (((duration-1)/duration) * prev_`i'__`=`y'-1') + ((1/duration) * prev_`i'__`=`y'-1'[_n-1]) if (age_start>5)
						replace prev_`i'__`y' = prev_`i'__`y' + (1 - prev_`i'__`y') * (1 - exp(-1 * inc_long_`i'__`y'))
						}
					}
						
				}
			quietly drop inc_long_`i'__* draw_`i'__*	
		
			di "." _continue
			}

		
			
			
		drop  age_end duration 
		
		
		* Reshape from wide back to long *
		forvalues y = 1990 / 2018 {
			preserve
			keep age_group_id age_start location_id sex_id model_idmeasure_id *`y'
			rename *`y' *
			generate year_id = `y'
			tempfile y`y'
			save `y`y''
	
			local append `append' `y`y''
			restore
			}
	
		clear 
		append using `append'
		
				
		rename prev_*__ prev_*
		
		drop if year_id==1989
		* Bring the raw draws back in to estimate prevalence of acute sequelae		
		merge 1:1  year_id age_group_id sex_id using `raw', assert(3) keep(match)
		
		* Add cases with acute sequelae (ie all cases but long-term cases), assuming 6 month duration
		forvalues i = 0 / 999 {
			replace prev_`i' = prev_`i' + (draw_`i' - (draw_`i' * (1 - hsaMean) * 0.476)) / 2	if age_start>=.5
			}
	
		drop draw_* 
		rename prev_* draw_*
		
		*make sure no negative draws
		
		forvalues i=0/999 {
			replace draw_`i'=0 if draw_`i'<0
			}
		
		
        keep measure_id location_id year_id age_group_id sex_id model_iddraw_* age_start age_end
		
		*keep estimation years
		keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2015, 2018)
		
		*replace measure_id for prevalence
		replace measure_id = 5
		replace model_id= "ADDRESS"
		export delimited using "`interms_dir'/1461_prevalence/`location'.csv", replace
		clear
