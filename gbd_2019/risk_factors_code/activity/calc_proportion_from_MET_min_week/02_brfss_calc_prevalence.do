// AUTHOR:
// DATE:
// PURPOSE: CLEAN AND EXTRACT PHYSICAL ACTIVITY DATA FROM BRFSS AND COMPUTE PHYSICAL ACTIVITY PREVALENCE IN 5 YEAR AGE-SEX GROUPS FOR EACH YEAR 

// NOTES: 
	// Revised to calculate estimates for each U.S. state 

// Set up
	clear all
	set more off
	set mem 2g
	if c(os) == "Unix" {
		global j "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global j "J:"
		version 11
	}


// args healthstate iso3 sex
	args us_state
	
// add logs
	log using "FILEPATH", replace 
	
	di "`us_state'"

// Bring in compiled file 
	use "FILEPATH", clear 

	keep if state == "`us_state'"

** *******************************************************************************************
** 2.) Calculate Prevalence in each year/age/sex subgroup and save compiled/prepped dataset
** *******************************************************************************************	

	
	// Set age groups
		//egen age_start = cut(age), at(25(5)120)
		replace age_start = 80 if age_start > 80 & age_start != .
		levelsof age_start, local(ages)

	// Remove those that have no recorded physical activity (zero edit)
		*drop if total_mets == 0

	// Make categorical physical activity variables
		gen inactive = total_mets < 600
		gen lowactive = total_mets >= 600 & total_mets < 4000
		gen lowmodhighactive = total_mets >= 600
		gen modactive = total_mets >= 4000 & total_mets < 8000
		gen modhighactive = total_mets >= 4000 
		gen highactive = total_mets >= 8000
		
		recode inactive lowactive lowmodhighactive modactive modhighactive highactive (0=.) if total_mets == .
		
	// Set survey weights
		svyset a_psu [pweight=a_finalwt], strata(a_ststr)
					
	//  Compute prevalence
		levelsof year_start, local(years)
		
		tempfile all 
		save `all', replace 

		di in red "`us_state'"

// Create empty matrix for storing proportion of a country/age/sex subpopulation in each physical activity category (inactive, moderately active and highly active)
			mata 
				state = J(1,1,"todrop") 
				year_start = J(1,1,999)
				age_start = J(1,1,999)
				sex = J(1,1,999)
				sample_size = J(1,1,999)
				inactive_mean = J(1,1,999)
				inactive_se = J(1,1,999)
				lowactive_mean = J(1,1,999)
				lowactive_se = J(1,1,999)
				modactive_mean = J(1,1,999)
				modactive_se = J(1,1,999)
				highactive_mean = J(1,1,999)
				highactive_se = J(1,1,999)
				modhighactive_mean = J(1,1,999)
				modhighactive_se = J(1,1,999)
				lowmodhighactive_mean = J(1,1,999)
				lowmodhighactive_se = J(1,1,999)
				total_mets_mean = J(1,1,999)
				total_mets_se = J(1,1,999)
			end	

		
		foreach year of local years {
				foreach sex in 1 2 {
					foreach age of local ages {
						
						use `all', clear 

						di in red  "state: `us_state' year:`year' sex:`sex' age:`age'"
						count if state == "`us_state'" & year_start == `year' & age_start == `age' & sex == `sex' & total_mets != . 
						local sample_size = r(N)
						
						if `sample_size' > 0 {
						
						// Calculate mean and standard error for each activity category	
							foreach category in inactive lowactive modactive highactive lowmodhighactive modhighactive total_mets {
								svy linearized, subpop(if state == "`us_state'" & year_start == `year' & age_start == `age' & sex == `sex'): mean `category'
								
								matrix `category'_meanmatrix = e(b)
								local `category'_mean = `category'_meanmatrix[1,1]
								mata: `category'_mean = `category'_mean \ ``category'_mean'
								
								matrix `category'_variancematrix = e(V)
								local `category'_se = sqrt(`category'_variancematrix[1,1])
								mata: `category'_se = `category'_se \ ``category'_se'
							}
							
							/*
							// Extract work activity level
								svy linearized, subpop(if state == "`us_state'" & year_start == `year' & age_start == `age' & sex == `sex'): mean workactive
								matrix meanmatrix = e(b)
								local mean = meanmatrix[1,1]
								mata: workactive_mean = workactive_mean \ `mean'
								
								matrix variancematrix = e(V)
								local se = sqrt(variancematrix[1,1])
								mata: workactive_se = workactive_se \ `se'
								*/

							// Extract other key variables	
								mata: state = state \ "`us_state'"
								mata: year_start = year_start \ `year'
								mata: sex = sex \ `sex'
								mata: age_start = age_start \ `age'
								mata: sample_size = sample_size \ `sample_size'
							
					}
					
				}
			}
		}

	// Get stored prevalence calculations from matrix
		clear
		getmata state year_start age_start sex sample_size highactive_mean highactive_se modactive_mean modactive_se lowactive_mean lowactive_se inactive_mean inactive_se lowmodhighactive_mean lowmodhighactive_se modhighactive_mean modhighactive_se total_mets_mean total_mets_se
		drop if _n == 1 // drop top row which was a placeholder for the matrix created to store results
	
	// Create variables that are always tracked		
		generate year_end = year_start
		generate age_end = age_start + 4
		egen maxage = max(age_start)
		replace age_end = 100 if age_start == maxage
		drop maxage
		gen GBD_cause = "physical_inactivity"
		gen national_type_id =  1 // nationally representative
		gen urbanicity_type_id = 1 // representative
		gen survey_name = "Behavioral Risk Factor Surveillance System"
		gen source = "micro_BRFSS"
		gen iso3 = "USA"
		gen questionnaire = "work_activity"
		// gen state = "`us_state'"
		
	// Replace standard error as missing if its zero
		recode inactive_se lowactive_se lowmodhighactive_se modhighactive_se modactive_se highactive_se total_mets_se (0 = .)
	
	//  Organize
		order iso3 state year_start year_end sex age_start age_end sample_size highactive* modactive* inactive* lowmodhighactive* modhighactive* total_mets*, first
		sort sex age_start age_end		
		
	// Save survey weighted prevalence estimates 
		save "FILEPATH", replace

	
