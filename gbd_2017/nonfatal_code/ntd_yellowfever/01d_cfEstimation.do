/*

NOTE:  This script uses the metan command, which is a user-written add on, and 
	   not part of base Stata.  Make sure to add metan to your Stata ado
	   library before running this.

*/	

*** BOILERPLATE ***
	set more off, perm
	clear

	if c(os) == "Unix" {
		local j "FILEPATH"
		set odbcmgr unixodbc
		}
	else if c(os) == "Windows" {
		local j "FILEPATH"
		}
		
		
	local inputDir FILEPATH
  
  
*** LOAD CASE FATALITY DATA **** 	 
	import delimited `inputDir'/caseFatality.csv, clear

	
*** GET DATA IN THE FORMAT THAT METAN PREFERS ***	
	gen mean = numerator / denominator
	gen lower = .
	gen upper = .

	forvalues i = 1/`=_N' {
		local n = numerator in `i'
		local d = denominator in `i'
		cii `d' `n', wilson
		replace upper = `r(ub)' in `i'
		replace lower = `r(lb)' in `i'
		}

*** RUN META-ANALYSIS AND CONVERT MEAN+SE TO BETA DISTRIBUTION PARAMETERS ***		
	metan mean lower upper, random nograph

	local mu    = `r(ES)'
	local sigma = `r(seES)'

	generate alphaCf = `mu' * (`mu' - `mu'^2 - `sigma'^2) / `sigma'^2 
	generate betaCf  = alpha * (1 - `mu') / `mu'

	
*** CLEAN UP AND SAVE ***	
	keep in 1
	keep alphaCf betaCf

	save `inputDir'/caseFatalityAB.dta, replace
