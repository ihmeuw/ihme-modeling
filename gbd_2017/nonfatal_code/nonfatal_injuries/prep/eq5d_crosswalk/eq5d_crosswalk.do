// USERNAME
// Clean Dutch Injury Data
// DATE

cap restore, not
clear
set mem 100m

// Derived from a follow-up survey administered to a sample from the Dutch Injury Surveillence System. 
use "${raw}/Dutch injury follow questionnaires_GBD.dta"

gen injdate = mdy(mndl, dagl, jaarl)

rename v12 EQ1_MO
rename v13 EQ1_SC
rename v14 EQ1_UA
rename v15 EQ1_PD
rename v16 EQ1_AD
gen EQ1days_since = v65 - injdate 
tostring EQ1_*, replace force
foreach var of varlist EQ1_* {
	replace `var' = "" if `var' == "9"
	replace `var' = "" if `var' == "."
}
egen EQ1_concat = concat(EQ1_*)
replace EQ1_concat = "" if length(EQ1_concat) != 5


rename w8  EQ2_MO
rename w9  EQ2_SC
rename w10 EQ2_DA
rename w11 EQ2_PD
rename w12 EQ2_AD
gen EQ2days_since = w49 - injdate 
tostring EQ2_*, replace force
foreach var of varlist EQ2_* {
	replace `var' = "" if `var' == "9"
	replace `var' = "" if `var' == "."
}
egen EQ2_concat = concat(EQ2_*)
replace EQ2_concat = "" if length(EQ2_concat) != 5

rename x8  EQ3_MO
rename x9  EQ3_SC
rename x10 EQ3_DA
rename x11 EQ3_PD
rename x12 EQ3_AD
gen EQ3days_since = x49 - injdate 
tostring EQ3_*, replace force
foreach var of varlist EQ3_* {
	replace `var' = "" if `var' == "9"
	replace `var' = "" if `var' == "."
}
egen EQ3_concat = concat(EQ3_*)
replace EQ3_concat = "" if length(EQ3_concat) != 5


rename z09  EQ4_MO
rename z10  EQ4_SC
rename z11  EQ4_DA
rename z12  EQ4_PD
rename z13  EQ4_AD
gen eq4date = mdy( z47b, z47a, z47c)
gen EQ4days_since = eq4date - injdate 
tostring EQ4_*, replace force
foreach var of varlist EQ4_* {
	replace `var' = "" if `var' == "9"
	replace `var' = "" if `var' == "."
}
egen EQ4_concat = concat(EQ4_*)
replace EQ4_concat = "" if length(EQ4_concat) != 5


// impute EQ date based on mean
forvalues i = 1/4 {
	replace EQ`i'days = . if EQ`i'days <= 0
	replace EQ`i'days = . if EQ`i'days > 2000
	summ EQ`i'days
	replace EQ`i'days = round(`r(mean)') if EQ`i'days == . & EQ`i'_concat != ""
}

// sex and age
rename  geslacht  sex
rename  leeftijd  age
replace age = . if age == 999

// id
tostring nummer, gen(id)

// hospital - admitted or not. 
recode opname (2=1) (1=0), gen(hospital)

// como
rename comorb como


// Data is mapped from EUROCOST 39 Injury Categories.
// each individual can have up to 3 n-codes 
foreach injury of varlist lgrp* {
	local no = subinstr("`injury'", "lgrp", "", .)
	gen gbd`no' = ""
	replace gbd`no' = ""  	if `injury' == 	0 // no injury
	replace gbd`no' = "N25"	if `injury' == 	1
	replace gbd`no' = "N26"	if `injury' == 	2
	replace gbd`no' = "N26"	if `injury' == 	3
	replace gbd`no' = "N1"	if `injury' == 	4
	replace gbd`no' = "N4"	if `injury' == 	5
	replace gbd`no' = "N4"	if `injury' == 	6
	replace gbd`no' = "N23"	if `injury' == 	7
	replace gbd`no' = "N2"	if `injury' == 	8
	replace gbd`no' = "N22"	if `injury' == 	9
	replace gbd`no' = "N3"	if `injury' == 	10
	replace gbd`no' = "N5"	if `injury' == 	11
	replace gbd`no' = "N4"	if `injury' == 	12
	replace gbd`no' = "N4"	if `injury' == 	13
	replace gbd`no' = "N7"	if `injury' == 	14
	replace gbd`no' = "N6"	if `injury' == 	15
	replace gbd`no' = "N6"	if `injury' == 	16
	replace gbd`no' = "N12"	if `injury' == 	17 // changed 11 sept 2012 from N1 to N12
	replace gbd`no' = "N1"	if `injury' == 	18
	replace gbd`no' = "N21"	if `injury' == 	19
	replace gbd`no' = "N4"	if `injury' == 	20
	replace gbd`no' = "N11"	if `injury' == 	21
	replace gbd`no' = "N9"	if `injury' == 	22
	replace gbd`no' = "N8"	if `injury' == 	23
	replace gbd`no' = "N10"	if `injury' == 	24
	replace gbd`no' = "N6"	if `injury' == 	25
	replace gbd`no' = "N6"	if `injury' == 	26
	replace gbd`no' = "N12"	if `injury' == 	27 // changed 11 sept 2012 from N1 to N12
	replace gbd`no' = "N1"	if `injury' == 	28
	replace gbd`no' = "N12"	if `injury' == 	29 // changed 11 sept 2012 from N1 to N12
	replace gbd`no' = "N1"	if `injury' == 	30
	replace gbd`no' = "N6"	if `injury' == 	31
	replace gbd`no' = "N1"	if `injury' == 	32
	replace gbd`no' = "N1"	if `injury' == 	33
	replace gbd`no' = "N14"	if `injury' == 	34
	replace gbd`no' = "N2"	if `injury' == 	35
	replace gbd`no' = "N1"	if `injury' == 	37
	replace gbd`no' = ""  	if `injury' == 	38  // NO INJURY
	replace gbd`no' = "N1"	if `injury' == 	39
}



// comorbidities -- very few defined in this dataset unfortunately. 
gen t_COPD_asthma			     = v10a
gen t_Heart_disease  		     = v10b
gen t_Stroke         		     = v10c
gen t_Diabetes       		     = v10d
gen t_Back_pain_hernia		     = v10e
gen t_Osteoarthritis  		     = v10f
gen t_Rheumatoid_arthritis       = v10g
gen t_Cancer					 = v10h
gen t_other 					 = v10j 



keep  injdate EQ1days_since EQ1_concat EQ2days_since EQ2_concat EQ3days_since EQ3_concat  EQ4days_since EQ4_concat hospital gbd* age sex id  t_*

// make dummies for each n-code
levelsof gbd1, local(ncodes)
foreach ncode of local ncodes {
	gen t`ncode' = 0
	forvalues i = 1/3 {
		replace t`ncode' = 1 if gbd`i' == "`ncode'"
	}
}
drop gbd*

// rename for reshape
forvalues i = 1/4 {
	rename EQ`i'days time`i'
	rename EQ`i'_concat EQ5D_concat`i'
}

reshape long EQ5D_concat time, i(id) j(round) string

// drop missing, which is most of the data (32080 of 42448 observations did not have a followup)
// drop if EQ5D == ""

destring round, replace
gen dutch = 1

// age groups
	// bin ages to 5 year age groups. start with 20.
	gen age_gr = .
	forvalues i = 5(5)75 {
		replace age_gr = `i' if age >= `i' & age <= (`i' + 4)
	}
	replace age_gr = 80 if age >= 80
	replace age_gr = . if age == .
	replace age_gr = 0 if age == 0
	replace age_gr = 1 if age >= 1 & age <= 4
	drop age


	// CROSSWALK - make response dummies for each possible eq5d value. 1s will be reference. 
	gen eq_mo2 = 1 if substr(EQ5D_concat,1,1) == "2"
	gen eq_mo3 = 1 if substr(EQ5D_concat,1,1) == "3"

	gen eq_sc2 = 1 if substr(EQ5D_concat,2,1) == "2"
	gen eq_sc3 = 1 if substr(EQ5D_concat,2,1) == "3"	
			
	gen eq_ua2 = 1 if substr(EQ5D_concat,3,1) == "2"
	gen eq_ua3 = 1 if substr(EQ5D_concat,3,1) == "3"
		
	gen eq_pd2 = 1 if substr(EQ5D_concat,4,1) == "2"
	gen eq_pd3 = 1 if substr(EQ5D_concat,4,1) == "3"
	
	gen eq_ad2 = 1 if substr(EQ5D_concat,5,1) == "2"
	gen eq_ad3 = 1 if substr(EQ5D_concat,5,1) == "3"
	
	foreach var of varlist eq_* {
		replace `var' = 0 if `var' == .
	}
	
	***************** METHOD 1: LOG OF SF ON EQ RESPONSE DUMMIES *******************************************************************
	// we will use this method. 
	
	append using "${int}/Log_Crosswalk_Dataset.dta"
	
	// convert from version 1 to version 2 SF-12
	gen PCS12xwalk = exp(PCSlog) + 1.07897
	gen MCS12xwalk = exp(MCSlog) - 0.16934
	
	replace MCSlog = log(MCS12xwalk)
	replace PCSlog = log(PCS12xwalk)	
	
	regress PCSlog eq_*, vce(robust)
		predict PCS_temp
		gen PCS_hat_log = exp(PCS_temp)

	regress MCSlog eq_*, vce(robust)
		predict MCS_temp
		gen MCS_hat_log = exp(MCS_temp)
	
	drop if dropcode == 1
	
	****************** METHOD 2: SF REGRESSED ON RE OF UNIQUE EQ COMBINATIONS
	
	merge m:1 EQ5D_concat using  "${int}/EQ-SF_unique_values_map.dta"
		drop _
		
	keep  id round sex age_gr tN* injdate time EQ5D_concat hospital dutch MCS_hat_unique PCS_hat_unique MCS_hat_log PCS_hat_log t_*
	
	
drop if id == ""
drop if EQ5D_concat == ""
save "${prepped}/1) Dutch_prepped_for_analysis.dta", replace



