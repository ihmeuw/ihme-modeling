

/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 10000
  set more off
  
  if c(os) == "Unix" {
	local j "/home/j"
	set odbcmgr unixodbc
	}
  else if c(os) == "Windows" {
	local j "J:"
	}
  

  local location `1'
  
  capture log close
  log using FILEPATH/`location'.smcl, replace
 
  adopath + FILEPATH
  adopath + FILEPATH 
  run FILEPATH/get_demographics.ado
  run FILEPATH/get_draws.ado
  
  tempfile remission appendTemp
  
  
* SETUP DIRECTORIES * 
  local rootDir FILEPATH
  
  
  

* CREATE EMPTY ROWS FOR INTERPOLATION *
  get_demographics, gbd_team(epi) clear
  local years `r(year_ids)'
  local ages  `r(age_group_ids)'
  
  clear
  set obs `=`=word("`r(years)'", -1)'-1979'
  generate year_id = _n + 1979	
  drop if (mod(year_id,5)==0 & inrange(year_id, 1990, 2010)) | year_id==2016
 
  
  generate age_group_id = 2
  foreach age of local ages {
	expand 2 if age_group_id==2, gen(newObs)
	replace age_group_id = `age' if newObs==1
	drop newObs
	}
  replace age_group_id = 235 if age_group_id==33

  bysort year_id age_group_id: generate sex_id = _n
  
  save `appendTemp'  
  
  
  
  
* GET GUILLIAN-BARRE REMISSION DRAWS *    
  get_draws, gbd_id_field(modelable_entity_id) gbd_id(2404) location_ids(`location') age_group_ids(`ages') measure_ids(7) source(dismod) status(best) clear
  rename draw_* remission_*
  drop modelable_entity_id model_version_id measure_id
 
 
  append using `appendTemp'

  fastrowmean remission_*, mean_var_name(remissionMean)	
	
  forvalues year = 1980/2016 {
	
	local index = `year' - 1979

	if `year'< 1990  {
	  local indexStart = 1990 - 1979
	  local indexEnd   = 2016 - 1979
	  }	
	else if inrange(`year', 2011, 2015) {
	  local indexStart = 31
	  local indexEnd   = 37
	  }
	else {
	  local indexStart = 5 * floor(`year'/5) - 1979
	  local indexEnd   = 5 * ceil(`year'/5)  - 1979
	  if `indexStart'==`indexEnd' | `year'==2016 continue
	  }

  
	foreach var of varlist remission_* {
		quietly {
		bysort age_group_id sex_id (year_id): replace `var' = `var'[`indexStart'] * exp(ln(remissionMean[`indexEnd']/remissionMean[`indexStart']) * (`index'-`indexStart') / (`indexEnd'-`indexStart')) if year_id==`year'
		replace  `var' = 0 if missing(`var') & year_id==`year'
        }
		
		di "." _continue
		}	
	}
  
	
  replace location_id = `location'

  save `remission'
  
  
  use `rootDir'/temp/allAge_`location'.dta, clear
  
  replace efAlpha = 4.817826 if location_id==422 
  replace efBeta  = 9.259581 if location_id==422
  
  
* CREATE ALL-AGE CASE DRAWS, ADJUSTING FOR UNDERREPORTING *  
  replace imported = 0 if missing(imported) | countryCases==0
  forvalues i = 0/999 {
  	generate gammaA = rgamma(efAlpha, 1)
	generate gammaB = rgamma(efBeta, 1)
	quietly generate draw_`i' = ((rbinomial(sample_size, ((cases + imported)/sample_size)) / (gammaA / (gammaA + gammaB))) * (pop_all_age / sample_size)) 
	drop gammaA gammaB
    }



* APPLY AGE/SEX PATTERNS TO CALCULATE AGE/SEX-SPECIFIC CASE DRAWS *	
  merge 1:m location_id year_id using FILEPATH/ageSpecific_`location'.dta, assert(3) nogenerate
  
  forvalues i = 0/999 {
	quietly {
	generate temp1 = exp(rnormal(ageSexCurve, ageSexCurveSe)) * pop_age_specific
	bysort location_id year_id: egen temp2 = total(temp1)
	replace draw_`i' = draw_`i' * temp1 / temp2
	drop temp1 temp2
	}
	di "." _continue
	}

	
* APPLY OUTCOMES SPLIT *	
  cross using  `rootDir'/temp/outcomes_`location'.dta

  forvalues i = 0/999 {
	quietly {
	replace draw_`i' = draw_`i' * multiplier if outcome=="inf_mod"
	replace draw_`i' = (draw_`i' / rbeta(alpha, beta)) - draw_`i' if outcome=="_asymp"
	replace draw_`i' = draw_`i' * prPreg / rbeta(alpha, beta) if outcome=="preg"   
	replace draw_`i' = draw_`i' * rbeta(alpha, beta) if inlist(outcome, "fatalities", "gbs")
	replace draw_`i' = 0 if missing(draw_`i')
	}
	di "." _continue
	}



  
* EXPORT DRAWS FOR NON-FATAL OUTCOMES OTHER THAN PREG *  
  preserve
  
  collapse (sum) draw_* pop_age_specific, by(location_id year_id age_group_id sex_id outcome modelable_entity_id)
  
  expand 2, generate(measure_id)
  replace measure_id = measure_id + 5
  
  merge m:1 location_id year_id age_group_id sex_id using `remission', assert(2 3) keep(3) nogenerate
  
  forvalues i = 0/999 {
	quietly {
		replace draw_`i' = draw_`i' / pop_age_specific
		replace draw_`i' = 0 if missing(draw_`i')
		replace draw_`i' = draw_`i' * 6/365 if measure_id==5 & inlist(outcome, "_asymp", "inf_mod") // Whitehead et al, doi: 10.1038/nrmicro1690
		replace draw_`i' = draw_`i' / remission_`i' if measure_id==5 & outcome=="gbs" // using remission draws from guillian-barre envelope dismod model
		}
	}
	 
 
  drop if measure_id==6 & outcome=="gbs" 
  
  foreach outcome in gbs _asymp inf_mod {
   export delimited location_id year_id age_group_id sex_id measure_id modelable_entity_id draw_*  using `rootDir'/`outcome'/`location'.csv if outcome=="`outcome'", replace

	}
	
  keep if inlist(outcome, "_asymp", "inf_mod") & measure_id==6
  collapse (sum) draw_*, by(location_id year_id age_group_id sex_id measure_id)
  generate modelable_entity_id = 10400
  export delimited location_id year_id age_group_id sex_id measure_id modelable_entity_id draw_*  using FILEPATH/`location'.csv, replace  

	
* ESTIMATE BIRTHS TO WOMEN INFECTED WITH ZIKA IN FIRST TRIMESTER *	
  restore
  keep if outcome=="preg" & sex_id==2
  collapse (sum) draw_*, by(location_id year_id)
  
  merge 1:m location_id year_id using FILEPATH/prBirthsBySex_`location'.dta
  
  expand 2 if year_id==1980, gen(newObs)
  replace year_id = year_id - newObs
  sort location_id year_id
  
  forvalues i = 0/999 {
	quietly {
	bysort sex_id (location_id year_id): gen temp = draw_`i'[_n-1]
	replace draw_`i' = ((draw_`i' * (52-24)/52) + (temp * 43/52)) * prBirthsBySex
	replace draw_`i' = 0 if missing(draw_`i')
	drop temp
	}
	}
	
  drop if year_id==1979
  generate age_group_id = 164

  export delimited location_id year_id age_group_id sex_id draw_*  using FILEPATH/`location'.csv, replace

  
  file open progress using FILEPATH/`location'.txt, text write replace
  file write progress "complete"
  file close progress
	  
  log close
