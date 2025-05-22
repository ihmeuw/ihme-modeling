* NTDs - Chagas disease 

/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 12000
  set more off

* PULL IN LOCATION_ID AND INCOME CATEGORY FROM COMMAND *  
 local location "ADDRESS
 local run_dir "ADDRESS

* LOAD SHARED FUNCTIONS *  
  run FILEPATH
  run FILEPATH
  run FILEPATH
  run FILEPATH
  
* SET UP LOCALS WITH MODELABLE ENTITY IDS AND AGE GROUPS *  

  local meid_master ADDRESS
  local meid_acute ADDRESS
  local meid_afib  ADDRESS
  local meid_hf    ADDRESS
  local meid_asymp ADDRESS
  local meid_total ADDRESS
  local meid_digest_mild ADDRESS
  local meid_digest_mod  ADDRESS
 
  tempfile master

  
* CREATE ZERO DRAW FILE FOR EXCLUDED LOCATIONS *
  get_demographics, gbd_team(ADDRESS) clear
  local age_group_id `r(age_group_id)'
  local year_id `r(year_id)'
  local sex_id `r(sex_id)'
  
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
  capture generate metric_id = 3
  
  destring sex_id, replace
  joinby age_group_id sex_id using FILEPATH
 
 
* EXPORT CHRONIC PREV ESTIMATES *
  levelsof model_id, local(meids) clean
   foreach meid of local meids {
       export delimited location_id year_id sex_id age_group_id metric_id measure_id model_iddraw_* using FILEPATH if model_id==`meid', replace
	   }

* CREATE AND EXPORT ASYMPTOMATIC FILE *	
  duplicates drop age_group_id sex_id year_id, force	   
  replace model_id= `meid_asymp'
	
  export delimited location_id year_id sex_id age_group_id measure_id metric_id model_iddraw_* using FILEPATH, replace

	
* CREATE AND EXPORT ACUTE FILE *	
  *duplicates drop age_group_id sex_id year_id, force
  expand 2, gen(newObs)
  replace measure_id = measure_id + newObs
  drop newObs

  foreach meid in ADDRESS ADDRESS {
	replace model_id= `meid'
  capture gen metric_id = 3
	export delimited location_id year_id sex_id age_group_id measure_id metric_id model_iddraw_* using FILEPATH, replace
	}
  
  file open progress using FILEPATH, text write replace
  file write progress "complete"
  file close progress
  
  log close
	
	 
