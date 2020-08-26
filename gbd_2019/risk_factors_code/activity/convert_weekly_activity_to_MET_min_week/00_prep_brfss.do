
// AUTHOR: 
// MODIFIED: 
// DATE: 
// DATE MODIFIED: 
// PURPOSE: CLEAN AND EXTRACT PHYSICAL ACTIVITY DATA FROM BRFSS AND COMPUTE PHYSICAL ACTIVITY PREVALENCE IN 5 YEAR AGE-SEX GROUPS FOR EACH YEAR 

// NOTES: 
	// Revised to calculate estimates for each U.S. state 


// Create locals for relevant files and folders
	local data_dir = "FILEPATH"
	local files: dir "`data_dir'" files "brfss_*"
	
	// local calc_prevalence = 1 // Turns on and off prevalence calculation code block
	
	set more off 

// Loop through files for each year
	foreach file of local files {
		use "`data_dir'/`file'", clear
		//renvars, lower
		foreach var of varlist * {
		cap rename `var' `=lower("`var'")' 
		}
		cap " //just adding this to fix syntax highlighting issue...
		local year = substr("`file'", -8, 4)
		cap drop if age < 25 | age == . // only estimate physical inactivity for ages 25+
		
// Years 2011 and 2013 (most recent BRFSS)
			if inlist("`year'", "2011", "2013") { 
				di in red "`year'" 
				cap rename a_age80 age 
				drop if age < 25 | age == . 
				
				if "`year'" == "2011" { 
					rename pamin_ total 
					rename pavigmn_ vig_total 
					gen mod_total = total - vig_total
				}

				if "`year'" == "2013" { 
					rename pa1vigm_ vig_total
					rename pa1min_ total 
					gen mod_total = total - vig_total 
				}
				
				// recode vig_total (.=0)
				gen mod_mets = mod_total * 4 
				gen vig_mets = vig_total * 8 
				egen total_mets = rowtotal(vig_mets mod_mets) 
				egen total_miss = rowmiss(vig_mets mod_mets) 
				replace total_mets = . if total_miss == 2
				drop total_miss
				label variable total_mets "domestic, transport, recreational"
				
				// Cross-check total hours accross all domains (shouldn't be active more than 16 hours a day, 7 days a week)
				egen total_time = rowtotal(vig_total  mod_total)
				replace total_mets = . if total_time > 6720
				drop total_time
				
				// Keep only necessary variables
					lookfor "a_strata" "a_ststr"
					local strata "`r(varlist)'"
					di "`strata'"
					
					lookfor "final weight" "a_finalwt" 
					foreach var in `r(varlist)' {
					local varlabel: variable label `var'
					if regexm("`varlabel'", "PRODUCT OF _POSTSTR") | regexm("`varlabel'", "FINAL WEIGHT")  {
							local pweight "`var'"
							di "`pweight'"
						}
					}
					
				lookfor "fips"
				local state "`r(varlist)'" 
				di "`state'"

				gen year_start = "`year'"
				destring year_start, replace
				cap rename a_llcpwt a_finalwt

				keep year_start sex age exerany2 *vig* *mod* total* a_psu a_finalwt exerany2 `strata' `state' genhlth

				tempfile brfss_`year'
				save `brfss_`year''
				
			}
			
			// YEARS 2001, 2002, 2003, 2005 2007 AND 2009 (Vigorous and moderate activities include all activity done outside of the occupational domain [i.e. recreation, transport and domestic])
			if inlist("`year'", "2001", "2002", "2003", "2005", "2007", "2009")  {
				di in red "`year'"
					foreach level in vigpa modpa {
						recode `level'day `level'tim (.=0) if `level'ct == 2 // making days per week and average min/day 0 if respondent does not perform moderate physical activity
						recode `level'day (77 88 99 = .) // should be missing
						recode `level'tim (777 999 = .) // should be missing
						
						tostring `level'tim, replace
						gen `level'_hrs = substr(`level'tim, -3, 1) // Get number of hours 
						gen `level'_min = substr(`level'tim, -2, 2) // Get number of minutes
						destring `level'_min `level'_hrs `level'tim, replace
						replace `level'_hrs = `level'_hrs * 60
						egen `level'_sum = rowtotal(`level'_min `level'_hrs)
						gen `level'_total = `level'_sum * `level'day
						recode `level'_total (0=.) if `level'day == . | `level'tim == .
					}
				
				// Calculate total MET - min/week  for vigorous and moderate activities and the total across both levels combined
					gen mod_mets = modpa_total * 4
					gen vig_mets = vigpa_total * 8
					egen total_mets = rowtotal(vig_mets mod_mets)
					egen total_miss = rowmiss(vig_mets mod_mets)
					replace total_mets = . if total_miss == 2 // should only exclude respondents with missing values for both moderate and vigorous physical activity, so as long as at least one level has valid answers and all other is missing, we will assume no activity at the other level
					drop total_miss
					label variable total_mets "domestic, transport, recreational"
				
				// Cross-check total hours accross all domains (shouldn't be active more than 16 hours a day, 7 days a week)
					egen total_time = rowtotal(vigpa_total  modpa_total)
					replace total_mets = . if total_time > 6720
					drop total_time
				
				// Keep only necessary variables
					lookfor "a_strata" "a_ststr"
					local strata "`r(varlist)'"
					di "`strata'"
			
					lookfor "final weight" "a_finalwt" 
					foreach var in `r(varlist)' {
						local varlabel: variable label `var'
						if regexm("`varlabel'", "PRODUCT OF _POSTSTR") | regexm("`varlabel'", "FINAL WEIGHT")  {
							local pweight "`var'"
							di "`pweight'"
						}
					}
					
					lookfor "fips"
					local state "`r(varlist)'" 
					di "`state'"
					
					if inlist("`year'", "2001", "2002") {
						keep sex age exerany2 *vig* *mod* total* a_rfpa* a_tot* jobactiv a_psu `pweight' `strata' `state' genhlth
					}
			
					else {
						keep sex age exerany2 *vig* *mod* total* a_rfpa* a_tot* a_rfparec a_rfpamod a_rfpavig a_rfnopa jobactiv a_psu `pweight' `strata' `state' genhlth
					}
					gen year_start = "`year'"
					destring year_start, replace
					gen work = 0
					gen rec = 1
					gen walk = 1
					gen domestic = 0
					
				tempfile brfss_`year'
				save `brfss_`year''
			}
		
			
		// YEARS 2004, 2006, 2008, 2010 & 2011 & 2012: 
			if inlist("`year'", "2004", "2006", "2008", "2010", "2012") {
				di in red "`year'"
				
				lookfor "a_strata" "a_ststr"
					local strata "`r(varlist)'"
					di "`strata'"
					
				lookfor "final weight" "a_finalwt" 
				foreach var in `r(varlist)' {
					local varlabel: variable label `var'
					if regexm("`varlabel'", "PRODUCT OF _POSTSTR") | regexm("`varlabel'", "FINAL WEIGHT")  {
							local pweight "`var'"
							di "`pweight'"
						}
					}
					
				lookfor "fips"
				local state "`r(varlist)'" 
				di "`state'"
			
				// Keep only necessary variables
					keep sex age exerany2 a_tot* a_psu `pweight' `strata' `state' genhlth
					if "`pweight'" == "a_llcpwt" {
						rename a_llcpwt a_finalwt 
					}
					recode exerany2 a_tot* (7 9 = .) (2=0)
					gen year_start = "`year'"
					destring year_start, replace
					
				tempfile brfss_`year'
				save `brfss_`year''
			}
		
		// YEARS PRIOR TO 2000
		 if regexm("`year'", "19") | "`year'" == "2000" { 
			di in red "`year'"
			// Keep only necessary variables	
				lookfor "a_strata" "a_ststr"
				local strata "`r(varlist)'"
				di "`strata'"	
				
				lookfor "final weight" "a_finalwt" 
				foreach var in `r(varlist)' {
					local varlabel: variable label `var'
					if regexm("`varlabel'", "PRODUCT OF _POSTSTR") | regexm("`varlabel'", "FINAL WEIGHT")  {
							local pweight "`var'"
							di "`pweight'"
						}
					}
					
				lookfor "fips"
				local state "`r(varlist)'" 
				di "`state'"
				
				if "`year'" == "2000" {
					keep sex age exer* a_totind* a_psu `pweight' `strata' `state' genhlth
				}
				else {
					cap keep sex age exer* a_totind* a_rflifes a_psu `pweight' `strata' `state' genhlth
				}
				gen year_start = "`year'"
				destring year_start, replace
					
				tempfile brfss_`year'
				save `brfss_`year''
		}
	}
	
// Append all years together
	use `brfss_1984', clear
	forvalues x = 1985/2013 {
		di `x'
		append using `brfss_`x''
	}
	
	rename genhlth general_health 
	recode general_health (7 9 = .)

// Define value labels for categorical variables
	label define pa_index 1 "Physically inactive"  2 "Irregular and/or not sustained activity" 3 "Regular and not intensive activity" 4 "Regular and vigorous activity" 9 "Unknown"
	label values a_totindx pa_index
	recode a_totindx (0 = 1) if exerany == 0 // Should be 1 (inactive) if respondent doesn't exercise
	recode a_totindx (0 9 = .)
	rename a_totindx pa_index
	
	recode a_rflifes (9 = .) // not applicable/refused
	label define pa_index2 1 "Did physical activity for 20 or more minutes, 3 or more times per week" 2 "No physical activity or less than 20 minutes of activity, 3 or more times per week" 
	label values a_rflifes pa_index2 
	rename a_rflifes pa_index2
	
	recode jobactiv (. = 0) if inlist(year_start, 2001, 2002, 2003, 2005, 2007, 2009) // Question not asked because respondent is not employed
	recode jobactiv (7 9 = .) //  Don't know/not sure, refused
	label define work_activity 0 "Not applicable, respondent does not work" 1 "Mostly sitting or standing" 2 "Mostly walking" 3 "Mostly heavy labor or physically demanding work" 
	label values jobactiv work_activity
	gen workactive = 1 if jobactiv ==3 // mostly heavy labor of physically demanding work
	replace workactive = 0 if inlist(jobactiv, 0, 1, 2)
	
	replace exerany = exerany2 if exerany == .
	drop exerany2
	recode exerany (2=0) (7 9 6 = .) 
	label define exerany 0 "No physical activity" 1 "Participates in physical activity"
	label values exerany exerany
	
// Make variables and variable names consistent with other sources 
	label define sex 1 "Male" 2 "Female"
	label values sex sex
	gen survey_name = "Behavioral Risk Factor Surveillance System"
	gen iso3 = "USA"
	gen year_end = year_start
	rename a_state state
	keep state age sex a_psu a_finalwt a_ststr exerany workactive jobactiv pa_index pa_index2 total_mets work rec domestic walk year_start year_end general_health 
	drop if total_mets == . & workactive == . // can't use if missing both MET-min/week and work activity level
	
	tempfile prep 
	save `prep', replace 
	
// Join with names of states 
	insheet using "`data_dir'/state_fips_codes.csv", comma clear
	
	merge 1:m state using `prep', keep(2 3 4 5) nogen 
	drop state 
	rename state_name state 
	
// Make 5-year age groups 
	egen age_start = cut(age), at(25(5)120)
	replace age_start = 80 if age_start > 80 & age_start != .
	levelsof age_start, local(ages)
	drop age

	tempfile compiled 
	save `compiled', replace

	/*

	
// Save cleaned and compiled raw dataset	
	save "FILEPATH/brfss_clean_revised.dta", replace
	
