// Purpose: 	append WHO Mort. Databank ICD versions together, format for consistency with other sources.

// setting up stata
	clear all 
	set mem 700m
	set more off
	
// creating a date-stamp local for archiving
	local date = c(current_date)
	
// setting directories
	if (c(os)=="Unix") global j "FILEPATH" 
	else local prefix "FILEPATH"
	cd "FILEPATH"
	global inputdir "FILEPATH"
	global outputdir "FILEPATH"
	
		global outputdir_old "FILEPATH"
	
	global ICD7 "WHO_MORTALITY_DATABASE_ICD7_Y2004M02D18.CSV"
	global ICD8 "WHO_MORTALITY_DATABASE_ICD8_Y2013M05D15.CSV"
	global ICD9 "WHO_MORTALITY_DATABASE_ICD9_Y2015M11D25.CSV"
	global ICD10_1 "WHO_MORTALITY_DATABASE_ICD10_PART_1_Y2016M09D15.CSV"
	global ICD10_2 "WHO_MORTALITY_DATABASE_ICD10_PART_2_Y2016M09D15.CSV"
	
** ***************************************
// getting full locations for GBD 2015
qui do "FILEPATH" 
get_locations, level(all)
tempfile locations
save `locations', replace
import excel using "FILEPATH", firstrow clear
merge 1:m location_name using `locations'
replace country = . if ihme_loc_id == "USA_533"
replace ihme_loc_id = "HKG" if ihme_loc_id == "CHN_354"
replace ihme_loc_id = "MAC" if ihme_loc_id == "CHN_361"
replace country = . if ihme_loc_id == "MEX_4657"
drop _m
rename country who_4_dgt_code
keep ihme_loc_id who_4 location_name
drop if who == .
save `locations', replace

** ***********************************************	
// appending datasets together
	cd "$inputdir"
	
	insheet using "$ICD7", clear
	tempfile ICD7
	save `ICD7', replace
	
	insheet using "$ICD8", clear
	tempfile ICD8
	save `ICD8', replace
	
	insheet using "$ICD9", clear
	tempfile ICD9
	save `ICD9', replace
	
	insheet using "$ICD10_1", clear
	tostring subdiv, replace
	gen version = "ICD10"
	tempfile ICD10
	save `ICD10', replace
	
	insheet using "$ICD10_2", clear
	gen version = "ICD10"
	replace sex = "1" if sex == "M"
	replace sex = "2" if sex == "F"
	destring sex, replace

	append using `ICD10'	
	
	append using `ICD7'
	replace version = "ICD7" if version == ""
	
	append using `ICD8'
	replace version = "ICD8" if version == ""
	
	append using `ICD9'
	replace version = "ICD9" if version == ""
	

** ************************************************
// basic formatting 
	// get rid of any variables that are all missing
	cap drop deahts16 
	sort year country version
	duplicates tag, g(d)
	
	// dropping duplicates in terms of all variables
	duplicates drop
	drop d
	
	// only keeping all cause death codes
	keep if (list=="07A" & cause == "A000") | (list=="07B" & cause == "B000") | (list=="08A" & cause == "A000") | ///
	(list=="08B" & cause == "B000") | (list=="09A" & cause == "B00") | (list=="09B" & cause == "B00") | ///
	(list=="09C" & cause == "C001") | (list=="09N" & cause == "B00") | (list=="101" & cause == "1000") | ///
	(list=="103" & cause == "AAA") | (list=="104" & cause == "AAA") | (list=="10M" & cause == "AAA") | (list=="UE1" & cause == "CH00")
	
	keep if (admin1 == . & subdiv == "") | (admin1 == . & subdiv == ".") | (admin == . & subdiv == "A20") | (admin == . & subdiv == "A35") | /// 
	(admin == . & subdiv == "A10")
	

	g VR_SOURCE = "WHO"
	g FOOTNOTE = "WHO Subdiv A20" if subdiv == "A20"			// reporting areas
	replace FOOTNOTE = "WHO Subdiv A35" if subdiv == "A35"		// selected urban and rural areas 
	
	// drop if sex is unknown
	drop if sex == 9
	
// saving raw dataset
	saveold "$outputdir/CRUDE_ALL_AGE_DEATHS_VR_WHO_1950-2011.dta", replace
		saveold "$outputdir_old/CRUDE_VR_WHO_1950-2011.dta", replace
	saveold "$outputdir/archive/CRUDE_ALL_AGE_DEATHS_VR_WHO_1950-2011_`date'.dta", replace
		saveold "$outputdir_old/Previous Versions of WHO raw VR/CRUDE_VR_WHO_1950-2011_`date'.dta", replace

** *********************************************************
// formatting dataset to match the formatted Mortality VR dataset
	drop list cause im_frmat admin1 im_*	
	rename country who_4_dgt_code
	rename sex SEX
	rename year YEAR
	rename subdiv SUBDIV


preserve
forvalues j = 1(1)26 {
	replace deaths`j' = -10000000000 if deaths`j' == .
}
collapse (sum) deaths*, by(who_4_dgt_code YEAR frmat SUBDIV VR_SOURCE FOOTNOTE version)
g SEX = 0
forvalues j = 1(1)26 {
	replace deaths`j' = . if deaths`j' < 0
}
tempfile bothsexes
save `bothsexes', replace
restore

append using `bothsexes'

**************************
// generating DATUM variables
 g DATUMUNK = deaths26
 g DATUMTOT = deaths1
 
 g DATUM1to4 = .
 forvalues j = 5(10)65 {
	local jplus = `j'+9
	g DATUM`j'to`jplus' = .
 }
 forvalues j = 0/5 {
	g DATUM`j'to`j' = .
 }
 forvalues j = 0(5)90 {
	local jplus = `j'+4
	g DATUM`j'to`jplus' = .
 }
 forvalues j = 65(5)95 {
	g DATUM`j'plus = .
 }

 
******************************************************
// adding deaths to DATUM variables based on formatting code 
// formatting 0
 forvalues j = 0/4 {
	local jplus = `j'+2
	replace DATUM`j'to`j' = deaths`jplus' if frmat == 0
}
forvalues j = 7/24 {
	local jlo = (`j'-6)*5
	local jhi = (`j'-6)*5+4
	replace DATUM`jlo'to`jhi' = deaths`j' if frmat == 0
}
replace DATUM95plus = deaths25 if frmat == 0
replace DATUM1to4 = deaths3 + deaths4 + deaths5 + deaths6 if frmat == 0
replace DATUM0to0 = deaths2 if frmat == 0 

// formatting 1
 forvalues j = 0/4 {
	local jplus = `j'+2
	replace DATUM`j'to`j' =deaths`jplus' if frmat == 1
}

forvalues j = 7/22 {
	local jlo = (`j'-6)*5
	local jhi = (`j'-6)*5+4
	replace DATUM`jlo'to`jhi' = deaths`j' if frmat == 1
}

replace DATUM85plus = deaths23 if frmat == 1
replace DATUM1to4 = deaths3 + deaths4 + deaths5 + deaths6 if frmat == 1
replace DATUM0to0 = deaths2 if frmat == 1

// formatting 2 
forvalues j = 7/22 {
	local jlo = (`j'-6)*5
	local jhi = (`j'-6)*5+4
	replace DATUM`jlo'to`jhi' = deaths`j' if frmat == 2
}
replace DATUM0to0 = deaths2 if frmat == 2
replace DATUM1to4 = deaths3 if frmat == 2
replace DATUM85plus = deaths23 if frmat == 2

// formatting 3 
 forvalues j = 0/4 {
	local jplus = `j'+2
	replace DATUM`j'to`j' =deaths`jplus' if frmat == 3
}
forvalues j = 7/20 {
	local jlo = (`j'-6)*5
	local jhi = (`j'-6)*5+4
	replace DATUM`jlo'to`jhi' = deaths`j' if frmat == 3
}
replace DATUM75plus = deaths21 if frmat == 3

// formatting 4
replace DATUM0to0 = deaths2 if frmat == 4
replace DATUM1to4 = deaths3 if frmat == 4
forvalues j = 7/20 {
	local jlo = (`j'-6)*5
	local jhi = (`j'-6)*5+4
	replace DATUM`jlo'to`jhi' = deaths`j' if frmat == 4
}
replace DATUM75plus = deaths21 if frmat == 4

// formatting 5
replace DATUM0to0 = deaths2 if frmat == 5
replace DATUM1to4 = deaths3 if frmat == 5
forvalues j = 7/19 {
	local jlo = (`j'-6)*5
	local jhi = (`j'-6)*5+4
	replace DATUM`jlo'to`jhi' = deaths`j' if frmat == 5
}
replace DATUM70plus = deaths20 if frmat == 5

// formatting 6
replace DATUM0to0 = deaths2 if frmat == 6
replace DATUM1to4 = deaths3 if frmat == 6
forvalues j = 7/18 {
	local jlo = (`j'-6)*5
	local jhi = (`j'-6)*5+4
	replace DATUM`jlo'to`jhi' = deaths`j' if frmat == 6
}
replace DATUM65plus = deaths19 if frmat == 6

// formatting 7 
replace DATUM0to0 = deaths2 if frmat == 7
replace DATUM1to4 = deaths3 if frmat == 7
forvalues j = 7(2)19 {
	local jlo = (`j'-6)*5
	local jhi = (`j'-6)*5+9
	replace DATUM`jlo'to`jhi' = deaths`j' if frmat == 7
}
replace DATUM75plus = deaths21 if frmat == 7 

// formatting 8
replace DATUM0to0 = deaths2 if frmat == 8
replace DATUM1to4 = deaths3 if frmat == 8
forvalues j = 7(2)17 {
	local jlo = (`j'-6)*5
	local jhi = (`j'-6)*5+9
	replace DATUM`jlo'to`jhi' = deaths`j' if frmat == 8
}
replace DATUM65plus = deaths19 if frmat == 8

drop frmat deaths*

g AREA = 0

// adding descriptors
	replace SUBDIV = "VR" if SUBDIV == ""
	replace SUBDIV = "VR" if SUBDIV == "."
	replace SUBDIV = "REPORTING AREAS" if SUBDIV == "A20"
	replace SUBDIV = "SELECTED URBAN AND RURAL AREAS" if SUBDIV == "A35"
	replace SUBDIV = "SURVEY" if SUBDIV == "A10"

sort who_4_dgt_code
merge m:1 who_4_dgt_code using `locations'
tab _merge
// only want the ones that are in the WHO data bank AND the ones that are reporting countries for GBD
drop if _merge == 2 | _merge == 1
drop _merge

drop who_4_dgt_code location_name

rename ihme COUNTRY
drop if COUNTRY == ""  

order COUNTRY YEAR SEX 
sort COUNTRY YEAR SEX

// dropping China
drop if regexm(COUNTRY, "CHN") == 1

// saving dataset and archived version
saveold "`prefix'FILEPATH", replace
saveold "`prefix'FILEPATH", replace

saveold "$outputdir_old/USABLE_VR_WHO_1950-2011.dta", replace 
saveold "$outputdir_old/Previous Versions of WHO raw VR/USABLE_VR_WHO_1950-2011_`date'.dta", replace

