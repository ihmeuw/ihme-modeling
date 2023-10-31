**NTDs Dengue
*process st/gpr all-age incidence output to apply age pattern


// Calculate prevalence of sequelae
// to the national level 
*** ======================= BOILERPLATE ======================= ***
	clear all
	set more off
	set maxvar 32000
	local user : env USER
	local code_root "FILEPATH"
    local data_root "FILEPATH"

	local params_dir "FILEPATH/params"
	local draws_dir "FILEPATH/draws"
	local interms_dir "FILEPATH/interms"
	local logs_dir "FILEPATH/logs_dir"
	local location_id			`10'

	cap log using "`logs_dir'/step_2a_`location_id'.smcl", replace
	if !_rc local close_log 1
	else local close_log 0

	// Source relevant functions
    local gbd_round_id "ADDRESS"
    local decomp_step "ADDRESS"

*** LOAD SHARED FUNCTIONS ***			
	adopath + FILEPATH  // source STATA
    run FILEPATH/get_demographics.ado
    run FILEPATH/get_population.ado
	
tempfile inc
*process st/gpr all-age incidence output to apply age pattern

*st/gpr draws are sourced from flat files after processing GRs for non-endemic locations

import delimited "FILEPATH/stgpr_draws_ADDRESS/`location_id'.csv"

*save tempfile
save `inc'

*get all age population for estimation years

get_population, location_id(`location_id') age_group_id(22) sex_id(1 2) year_id(1990 1995 2000 2005 2010 2015 2019 2020 2021 2022) decomp_step("ADDRESS") clear

*merge population to incidence file
merge 1:1 location_id year_id sex_id using `inc'

*convert into case space
forvalues i = 0/999 {
  quietly{
	replace draw_`i' = draw_`i'* population
	}
}


*KEEP LOCATION ID, YEAR ID AND POPULATION - PLUS DRAWS FROM FIXED EFFECTS
keep location_id year_id sex_id draw_* population 


*EXPAND TO CONSTRUCT AGE CATEGORIES - this creates rows to process the draws 
expand 25, generate(age_group_id)
bysort location_id year_id sex_id: generate counter= _n

replace age_group_id=counter
replace age_group_id=30 if counter==1
replace age_group_id=31 if counter==21
replace age_group_id=32 if counter==22
replace age_group_id=235 if counter==23
replace age_group_id=238 if counter==24
replace age_group_id=388 if counter==25
replace age_group_id=389 if age_group_id==5
replace age_group_id=34 if age_group_id==4
drop counter 
******************************************************************

**--------APPLY AGE/SEX PATTERN-------------------------**

* BRING IN AGE DISTRIBUTION DATASET *
rename population allAgePop

merge m:1 sex_id age_group_id using "FILEPATH/ageDistribution20.dta"

drop _merge

*MERGE POPULATION BY AGE 
merge 1:1 location_id year_id age_group_id sex_id using "FILEPATH/pop_age20.dta"

keep if _merge==3

*set early neonatal to zero per age restrictions (age_group_id=2)
replace incCurve = 0 if age_group_id==2
generate casesCurve = incCurve * population
bysort year_id location_id: egen totalCasesCurve = total(casesCurve)

forvalues i = 0/999 {
  quietly{
  replace draw_`i' = casesCurve * draw_`i' / totalCasesCurve
  replace  draw_`i' = draw_`i' / population
  }
   }
  *output from above lines is now incidence, not cases
 
keep year_id location_id age_group_id sex_id draw_*
generate model_id= "ADDRESS"
generate measure_id = 6
  
*LOOP THROUGH AND OUTPUT ALL DRAWS BY LOCATION_ID

export delimited location_id year_id sex_id age_group_id measure_id model_iddraw_* using FILEPATH/`location_id'.csv,  replace
