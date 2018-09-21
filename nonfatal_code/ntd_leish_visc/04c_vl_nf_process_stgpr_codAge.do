
*** BOILERPLATE ***
	clear all
	set maxvar 12000
	set more off
  
  
  
*** PULL IN LOCATION_ID AND ENDEMICITY CATEGORY FROM BASH COMMAND ***  
	local location   `1'
	local ageModel   `2'

  
  
*** START LOG ***  
	capture log close
	log using FILEPATH/`location'.smcl, replace
  
  
  
*** LOAD SHARED FUNCTIONS ***  
	adopath + FILEPATH
	run FILEPATH/get_draws.ado
	run FILEPATH/get_demographics.ado
	run FILEPATH/get_population.ado
  
  

  
*** SET UP LOCALS & TEMPFILES ***  MODELABLE ENTITY IDS AND AGE GROUPS ***  
	local outDir FILEPATH

	tempfile inc draws pop



 
  
*** PULL ALL-AGE INCIDENCE ESTIMATES FROM ST-GPR MODEL ***
	use `outDir'/temp/processing_`location'.dta, clear
	
	levelsof year_id, local(years) clean
	
	save `inc'
  
  
*** PULL THE DRAWS FROM THE INCDIENCE AGE/SEX CURVE MODEL ***  
	get_draws, gbd_id_field(cause_id) gbd_id(348) location_ids(`location') year_ids(`years') source(codem) clear
	drop if inlist(model_version_id, 361970, 361976) | age_group_id==22

	drop measure_id cause_id model_version_id envelope
	rename draw_* casesCurve_*
	rename pop population
  
  
  
*** CALCULATE TOTAL POPULATION ***  
	bysort location_id year_id: egen totalPop = total(population)
  
  
  
*** MERGE IN THE ALL-AGE INCIDENCE ESTIMATES ***   
	merge m:1 location_id year_id using `inc', assert(3) nogenerate

  
*** PROCESS INCIDENCE DRAWS AND APPLY AGE PATTERN ***  
	forvalues i = 0 / 999 {
		generate gammaA = rgamma(alpha, 1)
		generate gammaB = rgamma(beta, 1)
		generate draw_`i' =  totalPop * gammaA / (gammaA + gammaB)
		
		bysort location_id year_id: egen totalCasesCurve = total(casesCurve_`i')
		
		replace draw_`i' = endemic * casesCurve_`i' * (draw_`i' / totalCasesCurve) / population
			
		drop gammaA gammaB totalCasesCurve
		}
		
	
	
*** SPLIT INTO MEIDS ***
	expand 3
	bysort location_id year_id sex_id age_group_id: generate modelable_entity_id = _n + 1457
	
	expand 2 if modelable_entity_id!=1460, gen(measure_id)
	replace measure_id = measure_id + 5
	
	generate duration = 0.25 if modelable_entity_id==1458
	replace  duration = 0.1875 if modelable_entity_id==1459  
	replace  duration = 0.25 - 0.1875 if modelable_entity_id==1460
	
	forvalues i = 0 / 999 {
		quietly replace draw_`i' = draw_`i' * duration if measure_id==5
		di "." _continue
		}
		
	drop casesCurve_* population endemic alpha beta duration totalPop	
		
*** SAVE DRAW & PROGRESS FILES ***
	foreach meid in 1458 1459 1460 {
		export delimited location_id year_id age_group_id sex_id measure_id modelable_entity_id draw_* using `outDir'/`meid'/`location'.csv if modelable_entity_id==`meid', replace
		}
	
	file open progress using `outDir'/progress/`location'.txt, text write replace
	file write progress "complete"
	file close progress

	log close
