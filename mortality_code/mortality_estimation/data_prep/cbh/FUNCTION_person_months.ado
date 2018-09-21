** Description: This function formats complete birth history data in preparation for running the complete birth history code
** NOTE: IHME OWNS THE COPYRIGHT
** *************************************************************************

// Describe program syntax 
	cap program drop CBH_person_months
	program define CBH_person_months
		version 11
		syntax, directory(string) /// set directory 
				sex(string) /// set sex 
				neonatal(integer) /// calculate early and late (1) or just overall neonatal (2) 				
				[subset(integer 0)] /// run function for only one country 
				[subset_country(string)] /// which countries to run the function for (if subset = 1)
				[restart(integer 0)] /// should the function restart part way through the country list 
				[restart_country(string)] /// which country should the function restart at (if restart = 1) 
				[exclude(integer 0)] /// exclude countries 
				[exclude_country(string)] // which countries to exclude? (if exclude =1; note that this doesn't affect anything that has already been run) 

	/* this code assumes that each survey has already been prepped, and individual survey-country 
			combinations have been put in /SEX/svy_raw_sex.dta.
			
	   we will assume the following variables are in these files
			country - a string specifying the unit of interest (this doesn't need to actually be a country)
			svdate - the survey date in CMC 
			psu - the primary sampling unit (this code will make it unique across surveys in the same country so this is unnecessary to do earlier)
			sample_weight - the sample weights for the survey (which will be normalized in this code)
			birthdate - the birthdate for the child, in CMC
			death_age - the deathdate for the child, in months
			nn_death - an indicator specifying if a neonatal death was early or late (1 = early neonatal; 2 = late neonatal; 0 = no neonatal death) 
	*/ 
	
 

// Obtain list of all files to combine: 
	quietly {
	noi di "*********************************************************************"
	clear 
	local all_files: dir "strSurveyRawDataDir/`sex'/" files "*raw_`sex'.dta", respectcase
	gen filename = "" 
	foreach file of local all_files { 
		quietly { 
			count
			local set = `r(N)' + 1 
			set obs `set'
			replace filename = "`file'" if _n == _N 
		}
	} 
	split filename, parse("_")
	drop filename3 filename4 
	rename filename1 country
	rename filename2 sv_year
	gen svy = country + "_" + sv_year
	sort country sv_year

	// allow for restarting 
	if( `restart' == 1) {
		gen index = _n
		sum index if country == "`restart_country'"
		drop if index < r(sum)
		drop index
	}
  	
	// allow for subseting 
	if( `subset' == 1) {
		gen keep = 0 
		foreach cc of local subset_country { 
			replace keep = 1 if country == "`cc'"
		} 
		keep if keep == 1
		drop keep 
	}
	
	// allow for excluding
	if( `exclude' == 1) { 
		foreach cc of local exclude_country { 
			drop if country == "`cc'"
		} 
	} 

	tempfile filenames
	save `filenames', replace 
	
// Loop through files to create survey-specific datasets 
	noi display "Create Survey-Specific Datasets" 
	use `filenames', clear 
	levelsof svy, local(surveys)
	
	local total = _N 
	local count = 0 
	foreach svy of local surveys {
		local count = `count' + 1
		  
		use "strSurveyRawDataDir/`sex'/`svy'_raw_`sex'.dta", clear
		noi di "   survey `count' of `total'"	
		gen id=_n											// create an identifier for each child 
		expand 60											// expand each child to 60 observations - representing the 60 potential months lived in the first 5 years of life
		bysort id: gen age=_n-1								// generates age in months (1-60 for each child)
		bysort id: gen year=birthdate+(_n-1)				// generates observed age in cmc's of child at each of the 60 month time points
		bysort id: gen tps=svdate-year						// creates time-prior-to-survey based on the the svdate (cmcs) and the age of the child (cmcs)
		gen death=1 if death_age<=age						// code death=1 for children that died
		replace death=0 if death==.							// assigning 0 values for children who are alive/ and the months dead children lived
		drop if death==1 & age>death_age					// dropping extra months when children are dead
		drop if tps<=0										// dropping months not covered by the survey
		
	if (`neonatal' == 1) { 
		cap keep id psu sample_weight year age tps death nn_death
		order id psu sample_weight year age tps death nn_death
	} 
	
	if (`neonatal' == 2) { 
		cap keep id psu sample_weight year age tps death
		order id psu sample_weight year age tps death	
	}
	
		egen temp = sum(sample_weight)						// normalize the weights in each survey (this is to guarantee that all surveys have the same weight relative to each other) 
		replace sample_weight = sample_weight / temp 
		drop temp
		
		saveold "strSurveyRawDataDir/`sex'/`svy'_`sex'.dta", replace
	}
	
// Loop through countries to create country-specific datasets 
	noi display "Create Country-Specific Datasets" 
	use `filenames', clear 
	contract country
	summ _freq

	levelsof country, local(countries)

	local total = _N
	local country_count = 0 
	foreach country of local countries {
		local country_count = `country_count' + 1
		noi di "   country `country_count' of `total'"
		use `filenames', clear
		keep if country=="`country'"
		local sex="`sex'"
		local max = _N		
		forvalues i=1/`max' {
			local svy`i'=svy[`i']
		}
		tempfile temp
		local count = 0 
		forvalues i=1(1)`max' { 
			noi di "       svy `i' of `max'"
			local count = `count' + 1 
			use "strSurveyRawDataDir/`sex'/`svy`i''_`sex'.dta", clear 
			replace psu = (`i'*10^6) + psu // make psu unique across surveys
			if (`count' > 1) append using `temp'
			save `temp', replace
		} 
		saveold "strSurveyDataDir/`sex'/`country'_`sex'.dta", replace
	}
	
	} // close quietly 
	end 
		


