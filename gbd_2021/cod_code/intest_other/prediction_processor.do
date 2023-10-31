/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

*** BOILERPLATE ***
	clear all
	set maxvar 10000
	set more off

	adopath + /FILEPATH/stata

*** PULL IN LOCATION_IDs FROM BASH COMMAND ***  
	local cause_id `1'
	local location `2'
	local link `3'
	local min_age `4'
	local random `5'
	local saving_cause_ids = subinstr("`6'", "_", " ", .)
	local modelDir `7'
	local endemic `8'
	local inflator_draw_stub `9'

	
  
*** SET UP OUTPUT DIRECTORIES ***  
  	local rootDir  /FILEPATH
	*local causeDir `rootDir'/`cause_id'
	local tempDir `modelDir'/temp
	local logDir `modelDir'/logs
	local progressDir `logDir'/progress
	local codeDir /FILEPATH

	
*** START LOG ***  
	capture log close
	log using `logDir'/log_`location', replace
  
	macro dir
	
	
*** CREATE DRAWS ***
	use `tempDir'/`location'.dta, clear
	sort mergeIndex
	
	local dispersion = dispersion

	local zeroOut no
	if "`endemic'"!="0" {
		sum `endemic'
		if `r(mean)'==0 local zeroOut yes
		}
	
	forvalues i = 0 / 999 {
		quietly {
			generate draw_`i' = 0
			
			if "`zeroOut'"=="no" {			
				foreach covar of varlist covarTemp_* {
					replace draw_`i' = draw_`i' + (`covar' * beta_`covar'[`=`i'+1'])
					}
				
			
				if "`random'" == "yes" {
					foreach rVar of varlist randomMean* {
						local random = rnormal(0,1)
						replace draw_`i' = draw_`i' + (`random' * `=subinstr("`rVar'", "Mean", "Se", .)' + `rVar')
						*replace draw_`i' = draw_`i' + rnormal(`rVar', `=subinstr("`rVar'", "Mean", "Se", .)')
						}
					}
			
				generate drawRaw_`i' = `link'(draw_`i') * multiplier
				
				if "`inflator_draw_stub'"!="0" replace draw_`i' = draw_`i' + `inflator_draw_stub'`i'
				
				replace draw_`i' = `link'(draw_`i') * multiplier
				}
			}
		}
		
	if "`zeroOut'"=="no" {		
		fastrowmean drawRaw_*, mean_var_name(drawMeanRaw) 
		drop drawRaw_*
	
		forvalues i = 0 / 999 {
			quietly {
				
				replace draw_`i' = draw_`i' + ((runiform()*2)-1)
				replace draw_`i' = 0 if draw_`i'<0
				
				if "`dispersion'"=="constant"  {
					*replace draw_`i' = draw_`i' + ((runiform()*2)-1)
					*replace draw_`i' = 0 if draw_`i'<0
					generate gammaTemp = rgamma(draw_`i'/`link'(beta_dispersionTemp[`=`i'+1']), `link'(beta_dispersionTemp[`=`i'+1'])) 
					replace draw_`i' = gammaTemp if !missing(gammaTemp)
					drop gammaTemp
					}
			
				else if "`dispersion'"=="mean" {
					*replace draw_`i' = draw_`i' + ((runiform()*2)-1)
					*replace draw_`i' = 0 if draw_`i'<0
					generate gammaTemp = rgamma(1/`link'(beta_dispersionTemp[`=`i'+1']), draw_`i' * `link'(beta_dispersionTemp[`=`i'+1']))
					replace draw_`i' = gammaTemp if !missing(gammaTemp)
					drop gammaTemp
					}
				}
			}
			
		fastrowmean draw_*, mean_var_name(drawMeanAdj)
				
		forvalues i = 0 / 999 {
			quietly {
				replace draw_`i' = draw_`i' * drawMeanRaw / drawMeanAdj
				replace draw_`i' = 0 if age_group_id < `min_age'
				}
			}
		di "." _continue	
		}

	
	/*
	forvalues i = 0 / 999 {
		if "`random'" == "yes" generate draw_`i' = `link'(rnormal(fixed, fixedSe) + rnormal(random, randomSe)) * multiplier
		else generate draw_`i' = `link'(rnormal(fixed, fixedSe)) * multiplier
		replace draw_`i' = 0 if age_group_id <= `min_age' 
		}
	*/

	
*** SAVE DRAW FILE ***
	keep if keep==1 & !inlist(age_group_id, 22, 27)
	keep location_id year_id age_group_id sex_id draw_*
	foreach variable of varlist * {
		count if missing(`var')
		*sum `var'
		}
	ds *
	
	foreach saving_cause_id of local saving_cause_ids {
		generate cause_id = `saving_cause_id'
		outsheet using `modelDir'/`saving_cause_id'/draws/`location'.csv, comma replace
		drop cause_id
		}
	
	
*** SAVE PROCESS COMPLETION FILE ***
	file open progress using `progressDir'/`location'.txt, text write replace
	file write progress "complete"
	file close progress
   
	log close
  
  
  
