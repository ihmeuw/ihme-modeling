
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
  
  
* SETUP DIRECTORIES * 
  local rootDir FILEPATH
  
  import delimited using FILEPATH/`location'.csv, clear
  keep if measure_id==6
  duplicates drop location_id year_id sex_id age_group_id, force
  
  cross using FILEPATH/cfr_`location'.dta
  
  
* CREATE ALL-AGE CASE DRAWS, ADJUSTING FOR UNDERREPORTING *  
  local anyReportedDeaths = anyReportedDeaths

  forvalues i = 0/999 {
	local random = rnormal(0,1)
	generate gammaA = rgamma(alpha, 1)
	generate gammaB = rgamma(beta, 1)
	
	if `i'<26 & `anyReportedDeaths'==0 quietly replace draw_`i' = 0
	else if `anyReportedDeaths'==0 quietly replace draw_`i' = draw_`i' * (gammaA / (gammaA + gammaB)) * exp(`random' * randomSe + random) * 1.025 // adjustment to compenstate for zero draws
    else quietly replace draw_`i' = draw_`i' * (gammaA / (gammaA + gammaB)) * exp(`random' * randomSe + random)
	
	drop gammaA gammaB
	}

 
	
* EXPORT DEATH DRAWS *	
  generate cause_id = 935
  export delimited location_id year_id age_group_id sex_id cause_id draw_*  using FILEPATH/`location'.csv, replace
 
  file open progress using FILEPATH/`location'.txt, text write replace
  file write progress "complete"
  file close progress
	  
  log close
