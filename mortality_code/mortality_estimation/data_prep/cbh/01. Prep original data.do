// Purpose: This code obtains the relevant variables from Complete BH Questionnaires for direct 5q0 estimation
// This example is based off of a standard DHS Woman's individual recode file from Macro DHS 

	clear all
	set maxvar 32000
	set more off
	
	local country = "iso3Code"
	local svyear = "strSurveyYear"

** bring in the WN DHS module
  use "`filename'", clear
 
	// Make sure the variable names are consistent
	cap renpfix V v
	cap renpfix BORD bord
	cap renpfix B b
	cap drop b*_*_*
	
	cap keep v001 v005 v008 v021 bord_* b3_* b4_* b6_* b7_*
	local v021_error = _rc
	if( `v021_error' != 0 ) keep v001 v005 v008 bord_* b3_* b4_* b6_* b7_*
	else {
		qui inspect v021
		if ( r(N_unique) == 0 ) drop v021
		else drop v001
	}

	cap rename v001 psu
	cap rename v021 psu
  
	quietly {
		rename v005 sample_weight
		rename v008 svdate
	}
  
	quietly {
		gen mother_id = _n
		gen country = "`country'"
	}
  
	
  // BIRTH HISTORY CALCULATIONS
	foreach var in bord_ b3_ b4_ b6_ b7_ {
		qui renpfix `var'0 `var'
	}

	quietly {
		reshape long bord_ b3_ b4_ b6_ b7_, i(mother_id) j(sib)			// creates individual observations for each child
		drop if bord_ == .											    // drops empty rows where sib-number is not observed for a mother  
		drop sib
		rename bord_ sib
		rename b3_ birthdate
		rename b4_ sex
		rename b6_ death_age_fine
		rename b7_ death_age
		drop mother_id sib  
	}
  
	quietly {
		foreach var of varlist * {
			destring `var', replace
		}
	}

  order country svdate psu sample_weight sex birthdate death_age death_age_fine
      
	// Fixing death labels
		quietly {
			cap label list b7_20
			if( _rc == 0 ) {
				drop if death_age >= r(min) & death_age <= r(max)
				foreach x of numlist 1/20 {
					if(`x' >= 10) cap label drop b7_`x'
					else cap label drop b7_0`x'
				}
			}
		}
  
  // Format death age variables 
	replace death_age = (death_age_fine-300)*12 + 6 if death_age_fine > 300 	// center deaths recorded in years
	
		gen nn_death = 1 if death_age_fine >= 100 & death_age_fine <= 106		// mark early neonatal deaths
		replace nn_death = 2 if death_age_fine >= 107 & death_age_fine <= 127	// mark late neonatal deaths
		replace nn_death = 0 if nn_death == . 									// marke non-neonatal deaths
		replace death_age = 1 if death_age_fine > 127 & death_age == 0 			// move day 28-30 deaths into month 1 
		drop death_age_fine
	
  // Save data by sex of child
	qui cd "strFilePath"

	preserve
	drop sex
	qui saveold "both/`country'_`svyear'_raw_both.dta", replace
	restore
  
	preserve
	keep if sex == 1
	drop sex
	qui saveold "males/`country'_`svyear'_raw_males.dta", replace
	restore

	preserve
	keep if sex == 2
	drop sex
	qui saveold "females/`country'_`svyear'_raw_females.dta", replace
	restore
  

