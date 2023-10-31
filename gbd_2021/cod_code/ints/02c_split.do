

/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

*** BOILERPLATE ***
	clear all
	set maxvar 10000
	set more off

	adopath + FILEPATH
 
 
*** PULL IN LOCATION_ID AND INCOME CATEGORY FROM BASH COMMAND *** 
	local location "`1'"
	local modelMeid "`2'"
	local codModel "`3'"
	local step "`4'"

	local step STEP

	capture log close
	log using FILEPATH/log_`location', replace
  
  
*** SET UP OUTPUT DIRECTORIES ***  
	local rootDir FILEPATH


	
*** SET UP LOCALS WITH MODELABLE ENTITY IDS ***  
	local states inf_sev
	local inf_sev_meid 19680
	
	local cod_male MODEL_ID
	local cod_female MODEL_ID
	

	get_demographics, gbd_team(epi) clear
	local ages `r(age_group_id)'
	local ageList = subinstr("`ages'", " ", ",",.)
	local years `r(year_id)'
	local yearList = subinstr("`years'", " ", ",",.)

	
	get_demographics, gbd_team(cod) clear
	local codYears `r(year_id)'
	
	tempfile appendTemp mergeTemp

	
  
*** CREATE EMPTY ROWS FOR INTERPOLATION ***
	set obs `=max(`yearList') - 1979'
	generate year_id = _n + 1979
	drop if inlist(year_id, `yearList')

	generate age_group_id = 2

	foreach age of local ages {
		expand 2 if age_group_id==2, gen(newObs)
		replace age_group_id = `age' if newObs==1
		drop newObs
		}
		

	bysort year_id age_group_id: generate sex_id = _n

	save `appendTemp'
  
  
/******************************************************************************\
                      PULL IN DRAWS AND MAKE CALCULATIONS
\******************************************************************************/
  
*** PULL IN DRAWS FROM DISMOD INCIDENCE MODEL ***
	get_draws, gbd_id_type(modelable_entity_id) gbd_id(`modelMeid') location_id(`location') age_group_id(`ages') source(epi) status(best) decomp_step(`dm_step') clear  

	
	rename draw_* inc_*

	
	



    
	
/******************************************************************************\
                             INTERPOLATE DEATHS
\******************************************************************************/			

	append using `appendTemp'

	fastrowmean inc_*, mean_var_name(incMean)	


	forvalues year = 1980/`=max(`yearList')' {
		if inlist(`year', `yearList') continue
		
		local index = `year' - 1979
		


		if `year'< 1990  {
			local indexStart = 1990 - 1979
			local indexEnd   = `=max(`yearList')' - 1979
			}	
			
		else {
			tokenize "`years'"
			while `1'<`year' {
				local indexStart = `1' - 1979
				macro shift
				}
			local indexEnd = `1' - 1979
			}


		foreach var of varlist inc_* {
			quietly {
				bysort age_group_id sex_id (year_id): replace `var' = `var'[`indexStart'] * exp(ln(incMean[`indexEnd']/incMean[`indexStart']) * (`index'-`indexStart') / (`indexEnd'-`indexStart')) if year_id==`year'
				replace  `var' = 0 if missing(`var') & year_id==`year'
				}

			di "." _continue
			}	
		
		}
		
		
	replace location_id = `location'
	save `mergeTemp'



	
/******************************************************************************\
                    EXPORT FILES AND PERFORM SEQUELA SPLITS
\******************************************************************************/		
		

*** PULL IN HIV PREVALENCE DRAWS ***
	get_covariate_estimates, covariate_id(49) location_id(`location') year_id(`codYears') age_group_id(`ages') decomp_step(`step') clear 
	
	generate sigma = (upper_value - lower_value) / ( 2 * invnormal(0.975))
	generate alpha = mean_value * (mean_value - mean_value^2 - sigma^2) / sigma^2
	generate beta  = alpha * (1 - mean_value) / mean_value 
	
	replace  alpha = mean_value * 0.999e+8 if beta>0.999e+8
	replace  beta  = 0.999e+8 if beta>0.999e+8
	
	forvalues i = 0/999 {
		quietly {
			generate gammaA = rgamma(alpha, 1)
			generate gammaB = rgamma(beta, 1)
			
			generate hiv_`i' = gammaA / (gammaA + gammaB)
			replace  hiv_`i' = 0 if upper_value==0
			
			drop gamma*
			}
		}
		
		
	keep location_id year_id age_group_id sex_id hiv_*
	
	merge 1:1 location_id year_id age_group_id sex_id using `mergeTemp', assert(3) nogenerate
	save `mergeTemp', replace
			
  
*** MERGE IN CASE FATALITY DATA *** 
	file open dev using FILEPATH, read
	file read dev dev
	tokenize `dev'
	
	merge 1:1 location_id year_id age_group_id sex_id using FILEPATH, assert(3) nogenerate

		
	

*** PERFORM DRAW-LEVEL CALCULATIONS TO CALCULATE MRs *** N.B. PAF = (P*(RR-1)) / (P*(RR-1)+1) 

	forvalues i = 0/999 {
		quietly {
			local rr_hiv = rnormal(0, 1)
			
			generate rrTemp = exp(`rr_hiv' * lnHivRrSe + lnHivRrMean)
			replace  rrTemp = 1 if rrTemp < 1
			
			generate incNoHiv_`i' = inc_`i' * (1 - ((hiv_`i' * (rrTemp - 1)) / (hiv_`i' * (rrTemp - 1) + 1)))
			replace  incNoHiv_`i' = inc_`i' if incNoHiv_`i' > inc_`i'
			generate incHiv_`i'   = inc_`i' - incNoHiv_`i'
			replace  incHiv_`i'   = 0 if incHiv_`i'<0
			
						
			generate mrNoHiv_`i' = incNoHiv_`i' * invlogit(logitPred0 + (logitPredSeSm0 * `1'))
			generate mrHiv_`i'   = incHiv_`i'   * invlogit(logitPred1 + (logitPredSeSm1 * `1'))
			
			drop rrTemp
			macro shift
			}
		}		
		
		
		
*** EXPORT DEATHS ***
			
	replace measure_id = 1
	capture drop cause_id
	generate cause_id = 959

	rename mrHiv_* draw_* 
	outsheet location_id year_id sex_id age_group_id cause_id measure_id draw_* using FILEPATH, comma replace
	drop draw_*
		
	if "`codModel'"=="global" {	
		rename mrNoHiv_* draw_* 
		
		sort location_id age_group_id sex_id year_id
		foreach var of varlist draw_* {
			replace `var' = `var'[_n-1] if missing(`var')
			}
		
		outsheet location_id year_id sex_id age_group_id cause_id measure_id draw_* using FILEPATH if age_group_id>2, comma replace
		drop draw_*
		}
	
	else {
		drop mr*Hiv_*
		
		preserve
		
		tempfile codAppend
		
		get_draws, gbd_id_type(cause_id) gbd_id(959) source(codem)  version_id(`cod_male')  location_id(`location') age_group_id(`ages') year_id(`codYears') measure_id(1) decomp_step(`step') clear
		save `codAppend', replace
		
		get_draws, gbd_id_type(cause_id) gbd_id(959) source(codem)  version_id(`cod_female') location_id(`location') age_group_id(`ages') year_id(`codYears') measure_id(1) decomp_step(`step') clear
		append using `codAppend'
		
		forvalues i = 0 /999 {
			quietly replace draw_`i' = draw_`i' / pop
			di "." _continue
			}
			
		outsheet location_id year_id sex_id age_group_id cause_id measure_id draw_* using FILEPATH, comma replace
					
		restore	
		}
	
		
*** EXPORT HIV-SPECIFIC INCIDENCE ESTIMATES ***	
	capture drop cause_id
	replace measure_id = 6
	capture drop modelable_entity_id
	generate modelable_entity_id = 18651
			
	rename incNoHiv_* draw_* 
	outsheet location_id year_id sex_id age_group_id modelable_entity_id measure_id draw_* using FILEPATH, comma replace
	drop draw_*
	
	rename incHiv_* draw_* 
	outsheet location_id year_id sex_id age_group_id modelable_entity_id measure_id draw_** using FILEPATH, comma replace
	drop draw_*
	
	
	
	
*** PREP DATASET FOR NON-FATAL ***
	keep if inlist(year_id, `yearList')
	keep location_id year_id age_group_id sex_id inc_*
	rename inc_* draw_*
	

	
 	
	  
*** SPLIT OUT INCIDENCE AND CALCULATE PREVALENCE ***
	
	capture drop measure_id
	expand 2, gen(measure_id)
	replace measure_id = measure_id + 5
	
	foreach var of varlist draw_* {
		local duration = (rnbinomial(2, 0.21) + 1) / 365.25  // parameters derived using approximate Bayesian computation (ABC) conducted in "FILEPATH"
		quietly replace `var' = `var' * `duration'  if measure_id==5  
		
		di "." _continue
		}
	
		
		
		
*** EXPORT SEQUELA INCIDENCE DRAWS ***
	capture drop modelable_entity_id
	generate modelable_entity_id = .
	generate state = "inf_sev"
	
	foreach state of local states {
		replace modelable_entity_id = ``state'_meid'
		outsheet location_id year_id sex_id age_group_id modelable_entity_id measure_id draw_* using FILEPATH if state=="`state'", comma replace
		}


*** CLOSE LOG AND PROCESS FILES ***		
	file open progress using FILEPATH, text write replace
	file write progress "complete"
	file close progress

	log close
  
  
  