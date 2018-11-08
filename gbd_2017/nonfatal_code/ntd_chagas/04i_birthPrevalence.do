

	
/******************************************************************************\
      SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

*** BOILERPLATE ***
	clear all
	set maxvar 12000
	set more off
 


*** PULL IN LOCATION_ID AND ENDEMICITY CATEGORY FROM BASH COMMAND *** 
	local location "`1'"

 
*** START LOG *** 
	capture log close
	log using FILEPATH/bPrev_`location', replace
 
 
*** LOAD SHARED FUNCTIONS *** 
	adopath + FILEPATH

	local meid_acute 1451
	local outDir FILEPATH


	import delimited using FILEPATH/`location'.csv, clear
	append using FILEPATH/birthPrev_`location'.dta
	
	
	export delimited using FILEPATH/`location'.csv, replace
	
	
	file open progress using FILEPATH/`location'.txt, text write replace
	file write progress "complete"
	file close progress

	log close
