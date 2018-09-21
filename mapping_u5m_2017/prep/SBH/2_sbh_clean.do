/***********************************************************************************************************
 Author:
 Project: U5M
 Purpose: SBH data extraction

***********************************************************************************************************/


//////////////////////////////////
// Custom Indicator Generation
//////////////////////////////////

/////////////////////////////////////////////////////////////////
// Generate the variables which potentially require custom code//
/////////////////////////////////////////////////////////////////

/// For continuous variables (the missing values but be names `varname'_missing
foreach var in ceb ceb_male ceb_female ced ced_male ced_female children_alive children_alive_male children_alive_female males_in_house males_elsewhere females_in_house females_elsewhere {
	//If the indicator is in the dataset create a variable and replace missings with the missing values
	if !mi("$`var'") {
		cap gen `var' = $`var'
		destring `var', replace
		cap replace `var' = . if inlist(`var', $`var'_missing)
		}
	else {
		gen `var' = .
		}
}


///////////////////////////////////////////////////////
// Drop any remaining males and women not aged 15-49///
///////////////////////////////////////////////////////
cap drop if sex_id == 1
cap drop if age_year <15
cap drop if age_year >49

//////////////////////////////////////////////////////////////////
// Replace ceb = 0 if Y/N question statesno children ever born ///
//////////////////////////////////////////////////////////////////
if !mi("$given_birth") {
	replace ceb = 0 if $given_birth == $never_given_birth & !mi("$ceb")
	replace ceb_male = 0 if $given_birth == $never_given_birth & !mi("$ceb_male")
	replace ceb_female = 0 if $given_birth == $never_given_birth & !mi("$ceb_female")
	replace children_alive = 0 if $given_birth == $never_given_birth & !mi("$children_alive")
	replace children_alive_male = 0 if $given_birth == $never_given_birth & !mi("$children_alive_male")
	replace children_alive_female = 0 if $given_birth == $never_given_birth & !mi("$children_alive_female")
	replace ced = 0 if $given_birth == $never_given_birth & !mi("$ced")
	replace ced_male = 0 if $given_birth == $never_given_birth & !mi("$ced_male")
	replace ced_female = 0 if $given_birth == $never_given_birth & !mi("$ced_female")
	replace males_in_house = 0 if $given_birth == $never_given_birth & !mi("$males_in_house")
	replace females_in_house = 0 if $given_birth == $never_given_birth & !mi("$females_in_house")
	replace males_elsewhere= 0 if $given_birth == $never_given_birth & !mi("$males_elsewhere")
	replace females_elsewhere = 0 if $given_birth == $never_given_birth & !mi("$females_elsewhere")

	}

///////////////////////////////////////
// Any survey specific modifications //
///////////////////////////////////////

if "$nid" == "7575" {
		replace ceb = 0 if a1n1 > 1
		replace children_alive = 0 if a1n1 > 1
		}

if "$nid" == "7439" {
	replace ced_male = 0 if hngbgdie == 2
	replace ced_female = 0 if hngbgdie == 2
	}
/////////////////////////////////////////////////////////////////
// Calculate children born/dead/alive from available variabels///
/////////////////////////////////////////////////////////////////

//Calculate children alive from the number in the house and elsewhere if missing
if mi("$children_alive_male") & !mi("$males_in_house"){
	egen males_alive = rowtotal(males_in_house males_elsewhere)
	egen females_alive = rowtotal(females_in_house females_elsewhere)

	replace males_alive = . if males_in_house == . & males_elsewhere == . & females_in_house == . & females_elsewhere == .
	replace females_alive = . if males_in_house == . & males_elsewhere == . & females_in_house == . & females_elsewhere == .

	replace children_alive_male = males_alive
	replace children_alive_female = females_alive

	egen chalive = rowtotal(children_alive_male children_alive_female)
	replace chalive = . if children_alive_male ==. & children_alive_female == .
	replace children_alive = chalive if mi("$children_alive")
	drop chalive
	}

//Calculate children alive total if missing but males and females are present
if mi("$children_alive") & !mi("$children_alive_male") {
	egen chalive = rowtotal(children_alive_male children_alive_female)
	replace chalive  = . if children_alive_male == . & children_alive_female == .
	replace children_alive = chalive
	}

//Calculate children born from male and females if available and total isnt there
if mi("$ceb") & !mi("$ceb_male"){
	egen ceb_temp = rowtotal(ceb_male ceb_female)
	replace ceb_temp = . if ceb_male == . & ceb_female == .
	replace ceb = ceb_temp
}


//Calculate children died from male and females of available and total isnt
if mi("$ced") & !mi("$ced_male"){
	egen ced_temp = rowtotal(ced_male ced_female)
	replace ced_temp = . if ced_male == . & ced_female == .
	replace ced = ced_temp
}

// Calculate children died male and female if only children alive/born male and female available
if mi("$ced_male") & !mi("$ceb_male") & !mi("$children_alive_male") {
	replace ced_male = ceb_male - children_alive_male
	replace ced_female = ceb_female - children_alive_female
	}

// Calculate 8069CEB and CED from CEB/CED/CA as required
	if "$nid"=="8480"{
		replace ced = 0 if ced == . & children_alive !=.
		}
	egen ceb_temp2 = rowtotal(ced children_alive)
	replace ceb_temp2 = . if ced == . & children_alive == .
	replace ceb = ceb_temp2 if ceb ==.

	replace ced = ceb-children_alive if ced == .

//Ensure that dead and alive male and female are 0 when the totals are 0, instead of missing
replace ced_male = 0 if ced == 0 & ced_male == .
replace ced_female = 0 if ced == 0 & ced_female == .
replace children_alive_male = 0 if children_alive ==0 & children_alive_male ==.
replace children_alive_female = 0 if children_alive ==0 & children_alive_female ==.

// Calculate CEB male and female from dead and alive if missing
if mi("$ceb_male") {
	egen temp_ceb_male = rowtotal(children_alive_male ced_male) if ced_male !=. & children_alive_male !=.
	egen temp_ceb_female = rowtotal(children_alive_female ced_female) if ced_female !=. & children_alive_female !=.
	replace temp_ceb_male = . if children_alive_male == . & ced_male == .
	replace temp_ceb_female = . if children_alive_female == . & ced_female == .
	replace ceb_male = temp_ceb_male if ced_male !=. & children_alive_male !=.
	replace ceb_female = temp_ceb_female if ced_female !=. & children_alive_female !=.
}

// Replace any CED or children alive or CEB variables from datasets with 0 if 0 CEB (as some put these as missing)
foreach var in ceb_male ceb_female ced ced_male ced_female children_alive children_alive_male children_alive_female {
		replace `var' = 0 if ceb == 0
	}

/// Create an age group of woman variable
gen age_group_of_woman = .
if mi("$age") & !mi("$age_categorical"){
	replace age_group_of_woman  = 1 if age_categorical == "15 - 19"
	replace age_group_of_woman  = 2 if age_categorical == "20 - 24"
	replace age_group_of_woman  = 3 if age_categorical == "25 - 29"
	replace age_group_of_woman  = 4 if age_categorical == "30 - 34"
	replace age_group_of_woman  = 5 if age_categorical == "35 - 39"
	replace age_group_of_woman  = 6 if age_categorical == "40 - 44"
	replace age_group_of_woman  = 7 if age_categorical == "45 - 49"
	drop if age_group_of_woman  == .
	drop age_categorical
	}
else {
	replace age_group_of_woman = 1 if age_year >=15 & age_year<20
	replace age_group_of_woman = 2 if age_year >=20 & age_year<25
	replace age_group_of_woman = 3 if age_year >=25 & age_year<30
	replace age_group_of_woman = 4 if age_year >=30 & age_year<35
	replace age_group_of_woman = 5 if age_year >=35 & age_year<40
	replace age_group_of_woman = 6 if age_year >=40 & age_year<45
	replace age_group_of_woman = 7 if age_year >=45 & age_year<50
}
label define age_group 1"15-19" 2"20-24" 3"25-29" 4"30-34" 5"35-39" 6"40-44" 7"45-49"
label values age_group_of_woman age_group

drop males_in_house males_elsewhere females_in_house females_elsewhere children_alive children_alive_male children_alive_female

// If ceb or ced are > 30 replace with missing
replace ceb = . if ceb>30
replace ced = . if ced>30
replace ced = . if ceb>30
replace ced = . if ced>30
replace ced = . if ced < 0


//////////////////////////////////
// Recoding
//////////////////////////////////


//////////////////////////////////
// Validation
//////////////////////////////////
