
capture program drop resolver
program define resolver //, rclass
syntax anything(name=command id=command), estimate_type(string) age_groupFullVector(string) locationFullVector(string) yearFullVector(string) [by(string) covariate_name_short(string) covariate_name(string)]

quietly {

*** BOILERPLATE ***
	  local j /filepath

*** SETUP TEMPFILES ***
	tempfile master results resultsMaster missing
	tempfile locationResultsListTemp measureResultsListTemp age_groupResultsListTemp yearResultsListTemp sexResultsListTemp 
	tempfile age_groupListTemp locationListTemp yearListTemp sexListTemp

	
*** SAVE AND PROCESS MASTER DATASET ***	
	save `master'

	foreach x in age_group location year sex {
		preserve
		keep `x'_id
		duplicates drop
		
		save ``x'ListTemp'

		restore
		}
		
		
*** DETERMINE IF WE ARE PULLING MODEL RESULTS OR COVARIATES AND SET UP ENVIRONMENT ACCORDINGLY ***
	if "`estimate_type'" == "results" {
		run `j'/filepath/get_model_results.ado
		get_model_results, `command' location_set_id(35) decomp_step(step3) clear
		
		drop if inlist(age_group_id, 22, 27)

		replace measure = measure + "_" + string(model_version_id)
		local measure measure
		}


	else if "`estimate_type'" == "covariates" {
		run `j'/filepath/get_covariate_estimates.ado
		get_covariate_estimates, covariate_id(`command') decomp_step(step3) gbd_round_id(6)  clear
		
		keep location_id year_id `by' mean_value
		duplicates drop
		rename mean_value mean
		generate measure = 1
		}
		
	save `results'
	
	
*** CREATE VECTORS OF VALUES OF AGE, LOCATION, YEAR & SEX THAT ARE PRESENT IN RESULTS ***	
	foreach x in location year `=subinstr("`by'", "_id", "", .)' {
		levelsof `x'_id, local(`x'ResultsVector) clean
		}
	local minYear = min(`=subinstr("`yearResultsVector'", " ", ",", .)')

*** CROSS ALL COMBINATIONS OF MEASURE, LOCATION, AGE, YEAR & SEX FROM BOTH MASTER & RESULTS ***	
	keep measure
	duplicates drop
	save `resultsMaster'
	
	
	foreach x in location year `=subinstr("`by'", "_id", "", .)' {
		use `results', clear
		keep `x'_id
		append using ``x'ListTemp'
		duplicates drop
		cross using `resultsMaster'
		save `resultsMaster', replace
		}
	
	merge 1:1 location_id year_id `by' `measure' using `results', assert(1 3) nogenerate
	
	if "`estimate_type'" == "results" {
		merge m:1 location_id year_id age_group_id sex_id using `master', assert(1 3) generate(masterMerge)
		keep location_id year_id age_group_id sex_id measure* mean parent_id masterMerge
		}
	else if "`estimate_type'" == "covariates" {
		merge 1:m location_id year_id `by' using `master', assert(1 3) generate(masterMerge)
		keep location_id year_id age_group_id sex_id mean parent_id masterMerge
		duplicates drop 
		}
	

*** RESOLVE MISSING YEARS (EPI MODELS WILL HAVE 5-YEAR ESTIMATES) ***
	if "`: list yearFullVector - yearResultsVector'" != "" {
		noisily display " . Interpolating/extrapolating to create complete time-series"
		
		drop if year_id < `minYear'
  
	  	rename mean meanTemp

		bysort location_id age_group_id sex_id measure: ipolate meanTemp year_id, epolate gen(mean)
		
		expand `minYear' - 1979 if year_id==`minYear', gen(newObs)
		bysort location_id age_group_id sex_id measure newObs: replace year_id = _n + 1979 if newObs==1
		drop meanTemp newObs
		}
	
	else {
		noisily display " . Time-series is complete"
		}
	
	save `results', replace

  
*** RESOLVE MISSING LOCATIONS ***  
	if "`: list locationFullVector - locationResultsVector'" != "" { 
		noisily display " . Sorting out missing locations"

	  * FIND THE MISSING LOCATIONS ***
		generate missingLocation = location_id==`=subinstr("`: list locationFullVector - locationResultsVector'", " ", " | location_id==", .)'
		save `results', replace
		
	  * APPLY ESTIMATES FROM PARENT LOCATION TO ALL MISSING LOCATIONS ***
		keep if missingLocation==1
		generate merge_id = parent_id
		replace merge_id = 163 if parent_id==44538
		replace merge_id = 62 if inlist(parent_id, 44895, 44896, 44897, 44898, 44899, 44900, 44901, 44902)
		keep `measure' *_id
		save `missing'

		use `results'
		generate merge_id = location_id
		keep merge_id sex_id year_id age_group_id mean `measure'
		merge 1:m merge_id sex_id year_id age_group_id `measure' using `missing', keep(3) nogenerate
		save `missing', replace

		use `results', clear
		drop if missingLocation==1
		append using `missing'
		}
	
	else {
		noisily display " . No missing locations"
		}

		
*** RESOLVE MISSING AGE GROUPS *** 
	if "`: list age_groupFullVector - age_groupResultsVector'" != "" & strmatch("`by'", "*age_group_id*") {
		noisily display " . Sorting out missing age groups"
		replace mean = 0 if age_group_id==`=subinstr("`: list age_groupFullVector - age_groupResultsVector'", " ", " | age_group_id==", .)'
		}
		
	else if strmatch("`by'", "*age_group_id*") {
		noisily display " . No missing age groups"
		}
		
		
*** CLEAN UP & RESHAPE ***
	keep if masterMerge >= 3
	keep location_id year_id age_group_id sex_id `measure' mean 
	
	if "`estimate_type'" == "results" {
		reshape wide mean, i(location_id year_id age_group_id sex_id) j(measure) string
		foreach var of varlist mean* {
			label variable `var' "`covariate_name'"
			}
		ds mean*
		*return local new_covars `=subinstr("`r(varlist)'", "mean", "", .)'
		rename mean* *
		}
	else if "`estimate_type'" == "covariates" {
		label variable mean "`covariate_name'"
		rename mean `covariate_name_short'
		*return local new_covars `covariate_name_short'
		}

	}	

end
