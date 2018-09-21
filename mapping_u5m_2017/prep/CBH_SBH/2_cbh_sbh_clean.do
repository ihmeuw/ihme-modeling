/***********************************************************************************************************
 Author: 														
 Date: 07th Feb 2017
 Project: U5M
 Purpose: birthhistories topic code

***********************************************************************************************************/

//////////////////////////////////
// Subsets
/////////////////////////////////

// drop still born records which are included as 'cbh'
if "$nid" == "56099"{
	drop if q314 == 2
	}

if "$nid" == "21039"{
	keep if qb12a == 1
}

//////////////////////////////////
// Custom Indicator Generation
//////////////////////////////////

// create a mother_id variable
if !mi("$mother_id") {
	if regexm("$mother_id", ","){
		local m_id = subinstr("$mother_id", ",", "", .)
		egen mother_id = concat(`m_id'), punct(" ")
		}
		else {
		gen mother_id = $mother_id
		}
}

// create required variables
foreach var in admin_1 admin_2 {
	capture confirm new variable `var'
	if _rc == 0 {
		gen `var' = .
	}
}

/////// Generate continuous variables (incorporating the missing values, using the format `varname'_missing for fields containing missing values)
foreach var in ceb ced child_dob_cmc children_alive_num ceb_male ceb_female ced_male ced_female children_alive_male children_alive_female males_in_house males_elsewhere females_in_house females_elsewhere aod_number child_age_at_death_months child_age_at_death_raw childs_birth_month childs_birth_year mothers_age {
	if !mi("$`var'") {
		cap gen `var' = $`var'
		cap replace `var' = . if inlist(`var', $`var'_missing)
	}
	else {
		gen `var' = .
		}
 }

////// Generate categorical variables
// age at death units:
if !mi("$aod_units") {
	gen age_of_death_units = .
	replace age_of_death_units = 1 if $aod_units == $aod_units_days_vals
	replace age_of_death_units = 2 if $aod_units == $aod_units_months_vals
	replace age_of_death_units = 3 if $aod_units == $aod_units_years_vals
	}
	else{
	gen age_of_death_units = .
	}

/////////////////////////////////
// Create summary birth history variables
////////////////////////////////

// if 'children_alive_num' is missing, calculate using vars 'in house' and 'elsewhere'
	egen males_alive = rowtotal(males_in_house males_elsewhere) if "$males_in_house" != "" & "$males_elsewhere" != ""
	egen females_alive = rowtotal(females_in_house females_elsewhere) if "$females_in_house" != "" & "$females_elsewhere" != ""
	replace children_alive_male = males_alive if "$children_alive_male" == "" & "$males_in_house" != "" & "$males_elsewhere" != ""
	replace children_alive_female = females_alive if "$children_alive_female" == "" & "$females_in_house" != "" & "$females_elsewhere" != ""

	egen csurving = rowtotal(children_alive_male children_alive_female)
	replace children_alive_num = csurving if "$children_alive_num" == "" & "$males_in_house" != "" & "$males_elsewhere" != "" & "$females_in_house" != "" & "$females_elsewhere" != ""

// calculate ceb male and female from dead and alive if missing
	egen temp_ceb_male = rowtotal(children_alive_male ced_male) if "$children_alive_male" != "" & "$ced_male" != ""
	egen temp_ceb_female = rowtotal(children_alive_female ced_female) if "$children_alive_female" != "" & "$ced_female" != ""
	replace ceb_male = temp_ceb_male if "$ceb_male" == "" & "$ced_male" != "" & "$children_alive_male" != ""
	replace ceb_male = temp_ceb_male if "$ceb_male" == "" & "$ced_male" != "" & "$males_in_house" != "" & "$males_elsewhere" != ""
	replace ceb_female = temp_ceb_female if "$ceb_female" == "" & "$ced_female" != "" & "$children_alive_female" != ""
	replace ceb_female = temp_ceb_female if "$ceb_female" == "" & "$ced_female" != "" & "$females_in_house" != "" & "$females_elsewhere" != ""

// if ceb is missing in the raw data, calculate using 'ceb_male' and 'ceb_female' variables
	egen temp_ceb1 = rowtotal(ceb_male ceb_female) if "$ceb_male" != "" & "$ceb_female" != ""
	replace ceb = temp_ceb1 if "$ceb" == "" & "$ceb_male" != "" & "$ceb_female" != ""

// if ced is missing in the raw data, calculate using 'ced_male' and 'ced_female' variables
	egen temp_ced1 = rowtotal(ced_male ced_female) if "$ced_male" != "" & "$ced_female" != ""
	replace ced = temp_ced1 if "$ced" == "" & "$ced_male" != "" & "$ced_female" != ""

// if ced is still missing (male/female vars aren't available), calculate as ceb - children_alive_num
	gen temp2_ced2 = ceb - children_alive_num if "$ceb" != "" & "$children_alive_num" != ""
	replace ced = temp2_ced2 if "$ced" == "" & "$ceb" != "" & "$children_alive_num" != ""
	gen temp2_ced3 = ceb - csurving if "$ceb" != "" & "$males_in_house" != "" & "$males_elsewhere" != "" & "$females_in_house" != "" & "$females_elsewhere" != ""
	replace ced = temp2_ced3 if "$ced" == "" & "$ceb" != "" & "$children_alive_num" == "" & "$males_in_house" != "" & "$males_elsewhere" != "" & "$females_in_house" != "" & "$females_elsewhere" != ""

// if ceb variables are unavailable (ceb, ceb_male, ceb_female), calculate using ced and children_alive_num as required
	egen temp_ceb2 = rowtotal(ced children_alive_num), missing
	replace ceb = temp_ceb2 if ceb == . & ced != . & children_alive_num != .

// replace any ced or children_alive or ceb variables from datasets with 0 if ceb = 0 (as some put these as missing values)
foreach var in ceb_male ceb_female ced ced_male ced_female children_alive_num children_alive_male children_alive_female {
	if !mi("$`var'") {
		replace `var' = 0 if ceb == 0
		}
	}

// if no interview year is available, impute it as survey year
capture confirm new variable int_year
if _rc==0 {
	gen int_year = year_start if year_end - year_start == 0 // i.e the survey spans one year
	gen int_year = year_start if year_end - year_Start == 0 // i.e the survey spans two years (this may need a better fix...)
	gen int_year = year_start + 1 if year_end - year_start == 2 // i.e the survey spans three years (this may need a better fix...)
	}

// if no interview month is available, impute as the midpoint of the survey year
capture confirm new variable int_month
if _rc==0 {
	gen int_month = .
	replace int_month = 6 if year_end - year_start == 0 // i.e the survey spans one year
	replace int_month = 1 if year_end - year_start == 1 // i.e the survey spans 2 years (CHECK THAT THE INT YEAR IS SECOND YEAR)
	}

// correct interview date for specific PSE Health survey (2004)
// "The main fieldwork in the West Bank and Gaza Strip started on May 20, 2004, and was
// completed on July 7, 2004" - taking as July to reduce -ve birth to interview
if "$nid" == "20596"{
	replace int_month = 6
	}

// generate interview date cmc
gen interview_date_cmc = 12*(int_year-1900)+int_month

if "$nid" == "157018"{
	replace interview_date_cmc = v008
	}

// generate mothers age using birth month/birth year if age isn't a variable
if  mothers_age ==. {
	replace mothers_birth_month =. if mothers_birth_month > 12
	replace mothers_birth_year =. if mothers_birth_year > 2020
	gen m_dob_cmc = 12*(mothers_birth_year-1900)+mothers_birth_month
	gen m_age_imp = (interview_date_cmc - m_dob_cmc)/12
	replace m_age_imp = round(m_age_imp)
	replace mothers_age = m_age_imp if mothers_age ==.
}

gen mothers_age_group = .
replace mothers_age_group = 1 if mothers_age >=15 & mothers_age <=19
replace mothers_age_group = 2 if mothers_age >=20 & mothers_age <=24
replace mothers_age_group = 3 if mothers_age >=25 & mothers_age <=29
replace mothers_age_group = 4 if mothers_age >=30 & mothers_age <=34
replace mothers_age_group = 5 if mothers_age >=35 & mothers_age <=39
replace mothers_age_group = 6 if mothers_age >=40 & mothers_age <=44
replace mothers_age_group = 7 if mothers_age >=45 & mothers_age <=49

label define age_group 1"15-19" 2"20-24" 3"25-29" 4"30-34" 5"35-39" 6"40-44" 7"45-49"
label values mothers_age_group age_group

drop males_in_house males_elsewhere females_in_house females_elsewhere children_alive_num children_alive_male children_alive_female

////////////////////////////
// Survey specific adjustment
////////////////////////////

if "$nid" == "43016"{
	replace childs_birth_year = childs_birth_year + 1900
	}

// if no child dob is available, impute using birth month and year
	gen temp_cdob = 12*(childs_birth_year - 1900) + childs_birth_month
	replace child_dob_cmc = temp_cdob if "$child_dob_cmc" ==""

// correct interview date/childs dob to compute birth to interview
if "$nid" == "21301" {
	replace child_dob_cmc = child_dob_cmc+8
	replace interview_date_cmc = interview_date_cmc + 8
	}

if "$nid" == "19571" | "$nid" == "19557" {
	replace childs_birth_year = childs_birth_year + 7
	replace child_dob_cmc = 12*(childs_birth_year - 1900) + childs_birth_month
	replace child_dob_cmc = child_dob_cmc + 8
	replace interview_date_cmc = interview_date_cmc + 8
	}

if "$nid" == "20437" | "$nid" == "20450" {
	replace child_dob_cmc = child_dob_cmc+3
	replace interview_date_cmc = interview_date_cmc+3
	}

if "$nid" == "21240" | "$nid" == "20462" | "$nid" == "162317" {
	replace childs_birth_year = childs_birth_year -57
	replace child_dob_cmc = 12*(childs_birth_year - 1900) + childs_birth_month
	replace child_dob_cmc = child_dob_cmc+3
	replace interview_date_cmc = interview_date_cmc+3
	}

// If age_months hasnt been created as there was no interview date, recalculate using the imputed interview month
// capture confirm new variable age_month
// if _rc==0 {
//	gen age_month = interview_date_cmc - child_dob_cmc
//	replace age_month  = 0 if age_month <0
//	}

// rename age_month bti_cmc

// generate birth to interview; do this here instead of using 'age_months' as this var
// may represent mothers birth to interview, if using the WN files to merge
gen bti_cmc = interview_date_cmc - child_dob_cmc

//////////////////////////////////
// Create complete birth history variables
/////////////////////////////////

// create child age at death in months from 3 digit child_age_at_death_raw (dhs var)
	tostring child_age_at_death_raw, replace
	tostring age_of_death_units, replace
	tostring aod_number, replace
	replace age_of_death_units = "" if age_of_death_units == "."
	replace aod_number = "" if aod_number == "."
	replace age_of_death_units = substr(child_age_at_death_raw, 1, 1) if age_of_death_units =="" & child_age_at_death_raw !=""
	replace aod_number = substr(child_age_at_death_raw, 2, 3) if aod_number =="" & child_age_at_death_raw !=""
	replace aod_number = "" if aod_number == "99"
	destring age_of_death_units, replace
	destring aod_number, replace

// create age at death from units and number variables, if imputed is not available, and for neonatal
// replace the 0 month category with a decimal value (to portray days)[dhs]
if "$child_age_at_death_raw" != "" {
	replace child_age_at_death_months = aod_number/30 if age_of_death_units == 1 & aod_number !=.
	replace child_age_at_death_months = aod_number if age_of_death_units  == 2 & child_age_at_death_months == . & age_of_death_units !=.
	replace child_age_at_death_months = aod_number*12 if age_of_death_units == 3 & child_age_at_death_months == . & age_of_death_units !=.

}

// some manual manipulation for nids 10001 and 20596
if "$nid" == "10001" | "$nid" == "20596" {

	gen death_units = .
	replace death_units = 1 if re25d !=.
	replace death_units = 2 if re25m !=.
	replace death_units = 3 if re25y !=.

	gen death_value = .
	replace death_value = re25d if death_units == 1
	replace death_value = re25m if death_units == 2
	replace death_value = re25y if death_units == 3

	replace child_age_at_death_months = death_value/30 if death_units == 1 & "$child_age_at_death_months" == "" & death_units !=.
	replace child_age_at_death_months = death_value if death_units  == 2 & "$child_age_at_death_months" == "" & death_units !=.
	replace child_age_at_death_months = death_value*12 if death_units == 3 & "$child_age_at_death_months" == "" & death_units !=.

}

//////////////////////////////////
// Recoding
//////////////////////////////////

replace child_age_at_death_months = 6000 if child_alive == 1

/////////////////////////////////
// Add labels
/////////////////////////////////

// Label the binary variables
label define YN 0 "No" 1 "Yes"
label define sex  1"Male" 2"Female"
label define DMY 1 "Days" 2"Months" 3"Years"

cap label values child_alive YN
cap label values sex_id sex
cap label values age_of_death_units DMY

/////////////////////////////////
// Variable renaming to comply with current model formatting
////////////////////////////////

cap rename bti_cmc birthtointerview_cmc
cap rename sex_id childs_sex
cap rename age_year childs_age_years

//////////////////////////////////
// Further dropping of records
//////////////////////////////////
cap drop if mothers_age <15
cap drop if mothers_age >49

// if the number of ceb = 0, keep 1 row for the mother, but drop all others
duplicates tag mother_id, generate(dup_mid)
duplicates drop mother_id if ceb == 0, force

// drop if there's children with no line number (issue with reshape)
drop if dup_mid >0 & ceb != 0 & ced == 0 & child_no ==.
drop if dup_mid >0 & ceb != 0 & ced != 0 & child_no ==. & child_alive ==.

// drop rows for specific surveys
// GHA Special 2007-08 has alive/dead status for all children ever born, however
// the line number doesn't refer to live births, therefore, above script doesn't drop these rows, as child_no !=.
// below code deals with these instances

if "$nid" == "21173"{
	drop if dup_mid >0 & ceb != 0 & ced == 0 & child_alive ==.
	drop if dup_mid >0 & ceb != 0 & ced != 0 & child_alive ==.
	}

//////////////////////////////////
// Validation
//////////////////////////////////
