	
	
/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 12000
  set more off

* PULL IN LOCATION_ID AND INCOME CATEGORY FROM BASH COMMAND *  
  local location "`1'"

* START LOG *  
  capture log close
  log using FILEPATH/log_`location', replace
  
* LOAD SHARED FUNCTIONS *  
  adopath + FILEPATH
  run FILEPATH/get_draws.ado
  run FILEPATH/get_demographics.ado
  
* SET UP OUTPUT DIRECTORIES *  
  local outDir FILEPATH

  
* SET UP LOCALS WITH MODELABLE ENTITY IDS AND AGE GROUPS *  

  local meid_master 1450
  local meid_acute 1451
  local meid_afib  1452
  local meid_hf    2413
  local meid_asymp 3077
  local meid_total 10141
  local meid_digest_mild 1453
  local meid_digest_mod  1454
 
  tempfile master

  
* CREATE ZERO DRAW FILE FOR EXCLUDED LOCATIONS *
  get_demographics, gbd_team(epi) clear
  local age_group_id `r(age_group_ids)'
  local year_id `r(year_ids)'
  local sex_id `r(sex_ids)'
  
  foreach var in age_group_id year_id sex_id {
	clear
	set obs `=wordcount("``var''")'
	generate `var' = .
	forvalues i = 1 / `=wordcount("``var''")' {
		quietly replace `var' = `=word("``var''", `i')' in `i'
		}
	if "`var'"!="age_group_id" cross using `master'
	save `master', replace
	}

  forvalues i = 0 / 999 {
    generate draw_`i' = 0
	}
  order year_id sex_id age_group_id 	

  
  generate measure_id = 5
  generate location_id = `location'
  
  joinby age_group_id sex_id using `outDir'/inputs/chronicPr_`location'.dta
 
 
* EXPORT CHRONIC PREV ESTIMATES *
  levelsof modelable_entity_id, local(meids) clean
   foreach meid of local meids {
       export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using `outDir'/`meid'/`location'.csv if modelable_entity_id==`meid', replace
	   }

* CREATE AND EXPORT ASYMPTOMATIC FILE *	
  duplicates drop age_group_id sex_id year_id, force	   
  replace modelable_entity_id = `meid_asymp'
	
  export delimited location_id year_id sex_id age_group_id measure_id draw_* using `outDir'/`meid_asymp'/`location'.csv, replace

	
* CREATE AND EXPORT ACUTE FILE *	
  expand 2, gen(newObs)
  replace measure_id = measure_id + newObs
  drop newObs

  foreach meid in 1451 10141 {
	replace modelable_entity_id = `meid'
	export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using `outDir'/`meid'/`location'.csv, replace
	}
  
  file open progress using `outDir'/progress/`location'.txt, text write replace
  file write progress "complete"
  file close progress
  
  log close
	
	 
