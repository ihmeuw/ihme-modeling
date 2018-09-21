** ********************************************************************************************************************************************************************
** 	Adult Mortality through Sibling Histories: #3. Cleaning data to ready for analysis
**
**	Desription: This do-file conducts the first steps in preparing the raw sibling data for analysis.
**		Input: Output from steps 1 and 2 (allsibhistories.dta + missing sibs.dta)
**		Steps: 
**				1. Merge sibling information with the country identifier variables
**				2. Append missing sibs from zero survivor correction
**				3. Create new sibship and PSU IDs that are numeric, not string, to save space
**
**		Output: List of surveys included in analysis and a large dataset with all of the countries' sib modules with space-efficient sibship IDs and PSUs
**		File: allcountrysurveys_sexunk.dta
**		Variables in output dataset:		
**			iso3 country (full country name) yr_interview (Year of Interview) surveyyear (last full year for analysis) samplesize (Survey Sample Size) id_sm (sibship ID)
**			sibid (sibling ID) v005 (Sampling Weight) v008 (cmc date of interview) psu sex (0=Female 1=Male) yod (Year of death) yob (Year of birth) alive (Alive status) 
**
**	NOTE: IHME OWNS THE COPYRIGHT		 	
**
** ********************************************************************************************************************************************************************

** ***************************************************************************************
** SET UP STATA							
** ***************************************************************************************

clear
capture clear matrix
set more off

// Create a local of IHME country names and iso3 codes for merging later on 
use "$locdir/strLocationFileName.dta", clear
drop if (indic_epi != 1 & indic_cod != 1)
replace iso3 = gbd_country_iso3 + "_" + string(location_id) if gbd_country_iso3 != iso3 & gbd_country_iso3 != ""
duplicates drop
drop if iso3 == ""
keep iso3 location_name
rename location_name countryname_ihme

tempfile countrycodes
save `countrycodes', replace

// bring in initial file and create survey identifiers
use "$datadir/allsibhistories.dta", clear
	split svy, parse("_")
	rename svy1 survey
	rename svy2 iso3
	rename svy3 surveyyear
	capture looUSER svy4
	if !_rc replace surveyyear = svy4 if svy4 != "" // We dUSERt to the last year that the survey was valid

destring surveyyear, replace
replace surveyyear = surveyyear - 1		/* The "survey year" is actually one year prior to the survey, designating the last complete year of reporting. 
										Incomplete years cannot be used because the data structure is in person-years. If there is not a full
										year's exposure to deaths, death rates for incomplete years will be underestimated. */
sort iso3


** ***************************************************************************************
** 1. Merge sibling information with country identifier variables 
** ***************************************************************************************

merge m:1 iso3 using `countrycodes', keep(3) nogen

rename countryname_ihme country

** ***************************************************************************************
** 2. Append missing sibs from zero survivor correction
** ***************************************************************************************

gen missing_sib = 0
append using "$datadir/missing sibs.dta"
replace missing_sib = 1 if missing_sib == .
replace id = id + string(missing_sib)	// to make sure there aren't repeats in the original dataset and the missing sibling dataset

** ***************************************************************************************
** 3. Create new sibship and PSU IDs
** ***************************************************************************************

// This method was chosen because: 
//	1) Numeric variables will save space. 
//	2) If done separately by country, identifiers may not be unique when countries are pooled.
gen newid = id+"-"+svy
egen id_sm = group(newid)						

tostring surveyyear, gen(strsvyyr)
tostring v021, gen(strpsu) force
gen uniquepsu = country+"-"+strsvyyr+"-"+strpsu
egen psu = group(uniquepsu)

order country id_sm
sort country survey id_sm sibid

** ***************************************************************************************
** Drop variables that are no longer needed and save
** ***************************************************************************************
drop id newid strpsu strsvyyr v021 uniquepsu n
compress

compress

save "$datadir/allcountrysurveys_sexunk.dta", replace

