
*** BOILERPLATE ***		
	clear all
	set more off
	set maxvar 32000

	adopath + FILEPATH

	local location `1'
	local years = subinstr("`2'", "_", " ", .)
	local ages  = subinstr("`3'", "_", " ", .)
	local sexes 1 2
	local currentYear = max(`=subinstr("`years'", " ", ",", .)')

	local rootDir FILEPATH
	local outDir FILEPATH

	tempfile pop

	capture log close
	log using FILEPATH/log_`location'.smcl, replace 
	

	run FILEPATH/get_draws.ado
	run FILEPATH/get_population.ado

	sleep 100
	

*** PULL IN POPULTION ESTIMATES ***
	get_population, location_id(`location') age_group_id(`ages') sex_id(`sexes') year_id(`years') clear
	save `pop'
  
	sleep 100
  
  
*** PULL IN DEATH ESTIMATES ***
	get_draws, gbd_id_type(cause_id) gbd_id(359) source(ADDRESS) status(latest) location_id(`location') age_group_id(`ages') sex_id(`sexes') year_id(`years') measure_id(1) clear

	sleep 100
	
	merge 1:1 location_id year_id age_group_id sex_id using `pop', nogenerate
  
	keep location_id year_id age_group_id sex_id draw_* population
  
  
  
  
*** CONVERT DEATHS TO CASES ***
	forvalues i = 0/999 {
	  * The following line of code translates to Incidence = (Deaths + Survivers [assuming 99% CF]) / population 
	  quietly replace draw_`i' = (draw_`i' + rnbinomial(draw_`i',.99)) if draw_`i'>=0.0001  // rbinomial will return missing if draw is < 1e-4; therefore assume that deaths equals cases for these VERY small numbers
	  quietly replace draw_`i' =  draw_`i' / population
	  quietly replace draw_`i' = 0 if age_group_id<4
	  }
	  


*** CREATE PREVALENCE ESTIMATES ***	  
	expand 2, generate(measure_id)
	replace measure_id = measure_id + 5
   
	forvalues i = 0/999 {	  
		* The following line of code translates to Prevalence = Incidence * 2-weeks duration  
		quietly replace draw_`i' = draw_`i' * (2/52) if measure_id==5
		}


*** WRAP UP ***		
	generate modelable_entity_id = 1512

	export delimited location_id year_id age_group_id sex_id measure_id modelable_entity_id draw_* using FILEPATH/`location'.csv, replace


	file open progress using FILEPATH/`location'.txt, text write replace
	file write progress "complete"
	file close progress
  

	log close
