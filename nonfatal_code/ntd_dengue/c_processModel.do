	
/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

*** BOILERPLATE ***
	clear all
	set maxvar 12000
	set more off
  

*** PULL IN LOCATION_ID FROM BASH COMMAND ***  
	local location "`1'"

  
*** START LOG ***  
	capture log close
	log using FILEPATH/`location', replace
  
  
*** LOAD SHARED FUNCTIONS ***  
	adopath + FILEPATH
	run FILEPATH/get_draws.ado
	run FILEPATH/get_demographics.ado
  
  
*** SETUP A FEW LOCALS ***
	local rootDir FILEPATH
	tempfile appendTemp mergeTemp prevalence incidence chronic acute

	
*** LOAD INCIDENCE ESTIMATE FILE ***	
	use `FILEPATH/inc_`location'.dta, clear
	
	drop if year_id==2013



	
	
*** CREATE THE 1000 DRAWS ***
	forvalues i = 0/999 {
	local fixedTemp  = rnormal(0,1)
	local randomTemp = rnormal(0,1)
  
  	quietly {
		replace ef_`i' = ef_`i' / efMean
		generate draw_`i' = exp((`fixedTemp' * fixedSe + fixed) + (`randomTemp' * randomSe + random)) * ef_`i' * population  
		replace  draw_`i' = exp((`fixedTemp' * fixedSe + fixed) + (`randomTemp' * randomModelSe + randomModel)) * ef_`i' * population if inrange(randomModel, random, .) & !missing(mean)
		replace  draw_`i' = 0 if endemic==0 
		
		* We're estimating the implied expansion factor here (i.e. estimated/reported) to use in yf and zika models *
		generate efImplied_`i' = draw_`i' / cases if cases>=1000 
		replace  efImplied_`i' = 1 if ef_`i' < 1
		}
		
	di "." _continue
	}
	

*** EXPORT IMPLIED EXPANSION FACTORS ***
	preserve
	keep if inrange(year_id, 1990, .) & cases>=1000
	keep location_id year_id cases denguePr efImplied_*
	save FILEPATH/efImplied_`location', replace
	restore
	

	quietly sum is_estimate, meanonly
	if r(mean)>0 {
  
  
*** COLLAPSE ANNUAL ESTIMATES TO QUINQUENNIAL ESTIMATES *** 	
	fastcollapse draw_* population denguePr, by(location_id yearWindow) type(mean)
	rename yearWindow year_id
	replace year_id=2016 if year_id==2015  


*** BRING IN AGE DISTRIBUTION DATASET ***
	rename population allAgePop

	joinby year_id using FILEPATH/ageSpecific_`location'.dta

	keep year_id age_group_id sex_id location_id draw* allAgePop population incCurve denguePr
	replace incCurve = 0 if age_group_id<4

	generate casesCurve = incCurve * population
	bysort year_id location_id: egen totalCasesCurve = total(casesCurve)

	forvalues i = 0/999 {
		quietly {
			replace draw_`i' = casesCurve * draw_`i' / totalCasesCurve
			replace  draw_`i' = draw_`i' / population
			replace  draw_`i' = 1 if draw_`i'>1
			replace draw_`i' = 0 if denguePr==0
			}
		di "." _continue
		}
  

  
*** EXPORT ALL DENGUE CASE DRAWS ***
	keep year_id location_id age_group_id sex_id draw_*
	generate modelable_entity_id = 1505
	generate measure_id = 6
  
	export delimited using FILEPATH/`location'.csv, replace


 
  
*** SEQUELA SPLIT ***  
	keep year_id location_id age_group_id sex_id draw_*

	expand 3
	bysort year_id location_id age_group_id sex_id: generate modelable_entity_id = _n + 1505

	generate duration  = 6/365  if modelable_entity_id  == 1506  // Source of duration: Whitehead et al, doi: 10.1038/nrmicro1690
	replace  duration  = 14/365 if modelable_entity_id  == 1507  // Source of duration: Whitehead et al, doi: 10.1038/nrmicro1690
	replace  duration  = 0.5    if modelable_entity_id  == 1508 

	local u1506 = 0.945 
	local u1507 = 0.055 
	local u1508 = 0.084

	local s1506 = 0.074   
	local s1507 = 0.00765 
	local s1508 = 0.02 
  
	foreach meid in 1506 1507 1508 {
		local a`meid' = `u`meid'' * (`u`meid'' - `u`meid''^2 - `s`meid''^2) / `s`meid''^2
		local b`meid' = `a`meid'' * (1 - `u`meid'') / `u`meid''
		} 
  
	forvalues i = 0/999 {
		foreach meid in 1506 1507 1508 {
			local p`meid' = rbeta(`a`meid'', `b`meid'')
			}
		local correction = `p1506' + `p1507'	
		local p1506 = `p1506' / `correction' 
		local p1507 = `p1507' / `correction' 

		foreach meid in 1506 1507 1508 {
			quietly replace draw_`i' = draw_`i' * `p`meid''  if modelable_entity_id==`meid'
			}
		}
	
  
	expand 2, gen(measure_id)
	replace measure_id = measure_id + 5
  
	forvalues i = 0/999 {
		quietly replace draw_`i' =  draw_`i' * duration if measure_id==5
		}
	
	drop if modelable_entity_id==1508 & measure_id==6
	
*** EXPORT DRAW FILES ***		
	keep location_id year_id sex_id age_group_id modelable_entity_id measure_id draw_*  
  
	foreach meid in 1506 1507 1508 {
		export delimited FILEPATH/`location'.csv if modelable_entity_id==`meid', replace
		}
    
	}
	
	file open progress using FILEPATH/`location'.txt, text write replace
	file write progress "complete"
	file close progress

	log close
