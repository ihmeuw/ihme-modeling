/***********************************************************************************************************
 Author: 
 Project: CBH
 Purpose: Clean the CBH data for U5M model

***********************************************************************************************************/

//////////////////////////////////
// Custom Indicator Generation
//////////////////////////////////

// Generate any existing variables which require custom code

/// For continuous variables (the missing values but be names `varname'_missing
foreach var in child_aod_months child_aod_raw aod_number death_month death_year child_dod time_lived_days time_lived_months time_lived_years {
	//If the indicator is in the dataset create a variable and replace missings with the missing values
	if !mi("$`var'") {
		gen `var' = $`var'
		cap destring `var', replace
		cap replace `var' = . if inlist(`var', $`var'_missing)
		}
	else {
		gen `var' = .
		}
}

/// Drop records for still born children
if !mi("$born_alive"){
	gen born_alive = 0
	replace born_alive = 1 if inlist($born_alive, $born_alive_yes)
	drop if born_alive == 0
	}

// Had to shorten var names due to missing vals now rename
rename child_aod_months child_age_at_death_months
rename child_aod_raw child_age_at_death_raw
rename aod_number age_of_death_number

/// For categorical variables
// Age at death units:
if !mi("$age_of_death_units") {
	gen age_of_death_units = .
	replace age_of_death_units = 1 if $age_of_death_units == $aod_units_days_vals
	replace age_of_death_units = 2 if $age_of_death_units == $aod_units_months_vals
	replace age_of_death_units = 3 if $age_of_death_units == $aod_units_years_vals
	if !mi("$aod_units_weeks_vals") {
		replace age_of_death_units = 4 if $age_of_death_units == $aod_units_weeks_vals
		}
	}
	else{
	gen age_of_death_units = .
	}

// For string variabels which you want to be the meta
if !mi("$child_dod_format") {
	gen child_dod_format = "$child_dod_format"
	}
	else {
	gen child_dod_format = .
	}
//////////////////////////////////
// Recoding
//////////////////////////////////

// Label the binary variables
label define YN 0 "No" 1 "Yes"
label define sexMF  1"Male" 2"Female"

label values child_alive YN
label values sex_id sexMF


//If line ID doesnt exist make it the row number
capture confirm new variable line_id
if _rc==0 {
	gen line_id = _n
	}

// Generate child dob_cmc
gen child_dob_cmc = 12*(birth_year-1900)+birth_month

//If no interview month available impute midpoint of survey
capture confirm new variable int_month
if _rc==0 {
	gen int_month = .
	replace int_month = 6 if year_end - year_start == 0 // i.e the survey spans one year
	replace int_month = 1 if year_end - year_start == 1 // i.e the survey spans 2 years (CHECK THAT THE INT YEAR IS SECOND YEAR)
	}

// Generate interview_date_cmc from month and year output/imputed values
gen interview_date_cmc = 12*(int_year - 1900)+int_month

// If age_months hasnt been created as there was no interview date, recalculate using the imputed interview month
capture confirm new variable age_month
if _rc==0 {
	gen age_month = interview_date_cmc - child_dob_cmc
	replace age_month  = 0 if age_month <0
	}

//////////////////////////////////////////////////
/// Survey specific modifications/calculations ///
//////////////////////////////////////////////////


//If survey has been reshaped drop all records which dont relate to children
if !mi("$reshape_keepid") {
	keep if $reshape_keepid !=.
	}

// Adjust death year if in 2 digit format
if "$nid" == "4905" {
	replace death_year  = death_year +1900
	}

//Adjust dates which are not out by full years (remove if this is added to ubcov)
if "$nid" == "157018"{
	replace child_dob_cmc = child_dob_cmc+3
	replace interview_date_cmc = interview_date_cmc+3
	}

	if "$nid" == "19571" | "$nid" == "21301" | "$nid" == "19557" {
	replace child_dob_cmc = child_dob_cmc+8
	replace interview_date_cmc = interview_date_cmc+8
	}

if "$nid" == "20437" | "$nid" == "20450" | "$nid" == "39999" | "$nid" == "21240" | "$nid" == "20462" {
	replace child_dob_cmc = child_dob_cmc+3
	replace interview_date_cmc = interview_date_cmc+3
	}

//Adjust surveys which have an interview date in documentation / can be deduced
if "$nid" == "22674" {
	replace interview_date_cmc = interview_date_cmc-3
	replace age_month = interview_date_cmc - child_dob_cmc
	}
//////////////////////////////////////////////////////////////////////
///Calculating age at death months from various available variables///
//////////////////////////////////////////////////////////////////////

// Create child age at death in months from 3 digit child_age_at_death_raw
	tostring child_age_at_death_raw, replace
	tostring age_of_death_units, replace
	tostring age_of_death_number, replace
	replace age_of_death_units = "" if age_of_death_units == "."
	replace age_of_death_number= "" if age_of_death_number == "."
	replace age_of_death_units = substr(child_age_at_death_raw, 1, 1) if age_of_death_units =="" & child_age_at_death_raw !=""
	replace age_of_death_number = substr(child_age_at_death_raw, 2, 3) if age_of_death_number =="" & child_age_at_death_raw !=""
	destring age_of_death_units, replace
	destring age_of_death_number, replace

// If age at death is recorded as time_lived_years but no months and days - convert this to age at death number and units
if !mi("$time_lived_years") &  mi("$time_lived_months") & mi("$time_lived_days") {
	replace age_of_death_units = 3
	replace age_of_death_number = time_lived_years
	}

// If death year is provided but no death month then insert this into age at death units and number and calculate
if !mi("$death_year") & mi("$death_month") {
	replace age_of_death_units = 3 if age_of_death_units == .
	replace age_of_death_number = death_year-birth_year if age_of_death_number ==.
	}

/// Create age at death from units and number variables
	replace child_age_at_death_months = age_of_death_number/30 if age_of_death_units == 1 & child_age_at_death_months == . & age_of_death_units !=.
	replace child_age_at_death_months = age_of_death_number if age_of_death_units  == 2  & child_age_at_death_months == . & age_of_death_units !=.
	replace child_age_at_death_months = age_of_death_number*12 if age_of_death_units == 3 & child_age_at_death_months == . & age_of_death_units !=.
	replace child_age_at_death_months = age_of_death_number/4.45 if age_of_death_units == 4 & child_age_at_death_months == . & age_of_death_units !=.

// Create age at death from date of death
	gen cmc_death_date = 12*(death_year-1900)+death_month if child_age_at_death_months == . & death_year != .
	replace child_age_at_death_months = cmc_death_date - child_dob_cmc if child_age_at_death_months == . & death_year != .


// Create age at death months if given as days AND months AND years
if !mi("$time_lived_years") & !mi("$time_lived_months"){
    replace child_age_at_death_months = cond(missing(time_lived_days), 0, time_lived_days/30) + cond(missing(time_lived_months), 0, time_lived_months) + cond(missing(time_lived_years), 0, time_lived_years*12) if child_age_at_death_months == .
    replace child_age_at_death_months = . if time_lived_days == . & time_lived_months == . & time_lived_years == .
	}


////// Add any more formats  to change for age at death in here///////
if !mi("$child_dod") & "$child_dod_format" == "dd/mm/yyyy" {
	split $child_dod, parse("/") gen(temp)
	destring temp2, replace
	destring temp3, replace
	replace temp2 = . if temp2 >12
	replace temp3 = . if temp3 >3000
	replace cmc_death_date = 12*(temp3-1900)+temp2
	replace child_age_at_death_months = cmc_death_date - child_dob_cmc
}
////////////////////////////////////////////////////
/// Any model specific requirements for variables///
////////////////////////////////////////////////////

/// Recode child_age_at_death_months to 6000 for any children alive
replace child_age_at_death_months = 6000 if child_alive == 1


// Rename variables for use in U5M model
rename age_month birthtointerview_cmc
cap rename line_id child_id
rename sex_id child_sex

// Drop unwanted variables
drop child_dod_format time_lived_years time_lived_months child_dod death_year death_month age_year int_month int_year birth_month birth_year time_lived_days
//////////////////////////////////
// Validation
//////////////////////////////////

//Going to create a checklist to stop run if things arent right
// Child alive
// Birth to interview
// Child DOB
// Age at death
