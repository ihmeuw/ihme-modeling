// Authors: 	Jake Marcus and Kate Lofgren
// Purpose: 	Format the main VR database compiled by the IHME Cause of Death team into the standard format used for Death Distribution Methods and Mortality team processing.
// Edited by Carly 2/26/2014: edit to handle subnational data
// Edited by Megan 2/18/2015 to handle more subnationals

// setting up stata
	clear all
	set more off
	set mem 500m
	cap restore, not
	
if c(os)=="Windows" {
	local prefix=""
	}
if c(os)=="Unix" {
	local prefix=""
	}

global new_run_id = "`1'"

local run_folder = ""
local input_dir = ""
local output_dir = ""

local all_cause_dir = ""

// full locations for GBD
	import delimited using "", clear
	keep location_name ihme_loc_id location_type location_id
	gen gbd_country_iso3 = substr(ihme_loc_id, 1, 3) if inlist(location_type, "admin1", "admin2")
	keep location_name ihme_loc_id gbd_country_iso3 location_id
	duplicates drop
	tempfile countrycodes
	save `countrycodes', replace

cd "`all_cause_dir'"

local dropnid = "no"

use "", clear 
replace location_id = 152 if iso3 == "SAU"
replace NID = 287600 if NID == . //placeholder NID for cod VR

merge m:1 location_id using `countrycodes', keep(3)
drop gbd_country_iso3


// drop if sex is not male or female.  drop years that are not right (9999)
drop if sex == 9 | sex == 0
drop if year == 9999

	gen gbd_country_iso3 = iso3 if location_id != .
	
		// Hong Kong and Macao are their own countries: do not want to aggregate them into China
		replace iso3 = "HKG" if location_id == 354
		replace iso3 = "MAC" if location_id == 361
		replace gbd_country_iso3 = "" if inlist(iso3, "HKG", "MAC")
		replace location_id = . if inlist(iso3, "HKG", "MAC")
		replace ihme_loc_id = "HKG" if iso3 == "HKG"
		replace ihme_loc_id = "MAC" if iso3 == "MAC"
		// PRI is labelled as PRI and USA_385 (not modelled as part of US anymore)
		replace ihme_loc_id = "PRI" if location_id == 385
		replace gbd_country = "" if ihme_loc_id == "PRI"

		// only use ihme_loc_id to avoid errors with subnational locations
		drop iso3
		order ihme

// collapse across subdivisions 
	// 1. first check that iso3 sex year source is an identifier
	isid ihme_loc_id sex year source
	collapse (sum) deaths*, by(ihme_loc_id gbd_country_iso3 sex year source NID underlying_nid)
	

** *******************************************************************************************************
// dropping points that are OK for COD based on good Cause Fractions but not great for Mortality in overall sample size/ mortality rate
// is_mort_active is by nid-extract, and the following lines drops certain location-years and thus can't simply update is_mort_active for these.

// 1950 point in japan is unreasonably low
drop if ihme_loc_id == "JPN" & year == 1950

// Drop points where there is a larger than 1% difference with the WHO databank
drop if ihme_loc_id == "BRA" & year == 2009
drop if ihme_loc_id == "COL" & inlist(year, 1981, 1988, 1995, 1996, 2002, 2003, 2004, 2009)
drop if ihme_loc_id == "MEX" & year >= 1979 & year <= 2012
drop if ihme_loc_id == "MNG" & year >= 2004 & year <= 2008
drop if ihme_loc_id == "PER" & inlist(year, 2002, 2003, 2004, 2005)
drop if ihme_loc_id == "SAU" & year == 2009

** 3/9/2015 drop sources where the cause fractions are fine for CoD purposes but the death numbers are off for mortality
	** Romania, death numbers are too low pre-1969
	drop if ihme_loc_id == "ROU" & source == "ICD7A" & year <=1968
	** Honduras: the CoD source is much lower than the new WHO estimates for 1966
	drop if ihme_loc_id == "HND" & source == "ICD7A" & year == 1966

** 3/11/2015: drop CoD sources where the death numbers seem unlikely
	** Argentina 1966 and 1967, deaths are lower than trend
	drop if ihme_loc_id == "ARG" & source == "ICD7A" & inlist(year, 1966, 1967)
	** Israel: deaths are too low before 1974
	drop if ihme_loc_id == "ISR" & inlist(source, "ICD7A", "ICD8A") & year <=1974 & year >= 1954
	** Malaysia, deaths are much lower than previous VR
	drop if ihme_loc_id == "MYS" & (year >=1997 & year <=2008)
	** portugal: 2005/2006 are much lower than the trend
	drop if ihme_loc_id == "PRT" & source == "ICD10" & inlist(year, 2005, 2006)
	** UK regions huge spike in deaths in 1995 
	drop if ihme_loc_id == "GBR" & year == 1995
	** no CoD national total
	drop if ihme_loc_id == "BRA" & year < 2012

	** UKR 2013 NID 315739 is missing too much information, outlier it
	drop if ihme_loc_id == "UKR" & year==2013 & NID==315739


** **************************************************************************************	
// check that none of the variables are missing any death data		

	foreach var of varlist deaths2-deaths389 {

		count if `var' == .
		di in red "`var' has `r(N)' missing"
		assert `var' != .
	}

	foreach var of varlist deaths2-deaths389 {
		replace `var' = 0 if `var' == .
}

// format deaths into standard morality data structure: the data are in COD format 2
//  generating terminal age group - although all of this data is theoretically formatting in COD format 2
// there are often cases when deaths235 does not have any deaths -- Need to determine terminal agegroup manually
	gen terminal = .
	replace terminal = 235 if deaths235 != 0
// If no terminal age specified, have to find maximum age_group_end

	local age_ids = "235 32 31 30 20 19 18 17 16 15 14 13 12 11 10 9 8 7 6 34 238 389 388 2 3"
	foreach age of local age_ids {
		replace terminal = `age' if terminal == . & deaths`age' != 0
	}


	** Mark 70-74 as 70+ if that's the terminal group, e.g.
	local terminal_ages_new = "231 154 26 234 21 160 294"
	foreach age of local terminal_ages_new {
		gen deaths`age' = .
	}


	// If terminal group deaths are 0, replace with next highest 
	replace deaths231 = deaths17 if terminal==17
	replace deaths154 = deaths18 if terminal==18
	replace deaths26 = deaths19 if terminal==19
	replace deaths234 = deaths20 if terminal==20
	replace deaths21 = deaths30 if terminal==30
	replace deaths160 = deaths31 if terminal==31
	replace deaths294 = deaths32 if terminal==32

// format deaths into standard mortality data structure, IHME age groups
// 5-20 continuous, then have to append 2, 3, 388, 389, 28, 238, 34, 30, 31, 32, 235 manually
	forvalues j = 6/20 {
		local k = (`j'-5)*5
		local k4 = `k' + 4

		cap: gen DATUM`k'to`k4' = .
		replace DATUM`k'to`k4' = deaths`j' if `j' < terminal

		cap: gen DATUM`k'plus = .
		replace DATUM`k'plus = deaths`j' if `j' == terminal
	}

	gen DATUMenntoenn = deaths2
	gen DATUMlnntolnn = deaths3
	gen DATUMpnatopna = deaths388
	gen DATUMpnbtopnb = deaths389
	gen DATUMpostneonatal = deaths4
	gen DATUM1to1 = deaths238
	gen DATUM2to4 = deaths34
	gen DATUM1to4 = deaths5
	gen DATUM80to84 = deaths30
	gen DATUM85to89 = deaths31
	gen DATUM90to94 = deaths32
	gen DATUM95plus = deaths235

	// Add pnn, age group 5 if missing
	replace DATUMpostneonatal = deaths388 + deaths389 if DATUMpostneonatal == . & deaths388 != . & deaths389 != .
	replace DATUM1to4 = deaths238 + deaths34 if DATUM1to4 == . & deaths238 != . & deaths34 != .

	// handle 60 plus, 65 plus, ... 90 plus
	forvalues j= 60(5)90 {
		cap gen DATUM`j'plus = .
	}

	replace DATUM60plus = deaths231 if terminal==231 | terminal==17
	replace DATUM65plus = deaths154 if terminal==154 | terminal==18
	replace DATUM70plus = deaths26 if terminal==26 | terminal==19
	replace DATUM75plus = deaths234 if terminal==234 | terminal==20
	replace DATUM80plus = deaths21 if terminal==21 | terminal==30
	replace DATUM85plus = deaths160 if terminal==160 | terminal==31
	replace DATUM90plus = deaths294 if terminal==294 | terminal==32

	// If 80 plus is terminal group, 70plus/90plus/etc. should be 0, and 80-84
	forvalues k = 80(5)90 {
		replace DATUM`k'plus = 0 if DATUM`k'plus == .
		local kplus = `k' + 4
		replace DATUM`k'to`kplus' = 0 if DATUM`k'plus != 0
	}

	replace DATUM45plus = 0 if DATUM45plus == .
	replace DATUM70plus = 0 if DATUM70plus == .

	
	lookfor DATUM
	foreach var of varlist `r(varlist)' {
		summ `var'
		if `r(N)' == 0 {
			drop `var'
		}
	}

	drop deaths* 
	cap drop DATUM95to99

*************************
// Step 2: general formatting 

	// merge in country name
	merge m:1 ihme_loc_id using `countrycodes'
	replace location_name = "Hong Kong Special Administrative Region of China" if ihme_loc_id == "HKG"
	replace location_name = "Macao Special Administrative Region of China" if ihme_loc_id == "MAC"
	** assert _m !=1
	drop if _m == 2			// this drops non-ihme indicator countries and observations that are specific to DEU-WB
	drop _m

	gen VR_SOURCE = "WHO_causesofdeath"
	rename ihme_loc_id COUNTRY
	rename sex SEX
	rename year YEAR
	g FOOTNOTE = ""
	
	// data check 
	sort COUNTRY YEAR SEX source NID
    quietly by COUNTRY YEAR SEX:  gen dup = cond(_N==1,0,_n)
	preserve
	keep if dup>0
	saveold "", replace
	restore
	drop if dup>1 

	drop dup
	
	drop source terminal location_name gbd_country_iso3 location_id
	
	// add in unknown deaths and total deaths
	cap g DATUMUNK = .
	cap drop DATUMTOT
	egen DATUMTOT = rowtotal(DATUM*)
	assert DATUMTOT != .
	
	assert inlist(SEX, 1, 2)
	// add in all sexes combined
	preserve
		lookfor DATUM
		foreach var of varlist `r(varlist)' {
			replace `var' = -10000000 if `var' == .
		}
		collapse (sum) DATUM*, by(COUNTRY YEAR VR_SOURCE FOOTNOTE NID underlying_nid)
		gen SEX = 0

		lookfor DATUM
		foreach var of varlist `r(varlist)' {
			replace `var' = . if `var' < 0
		}	

		tempfile both
		save `both', replace
	restore

	append using `both'

// more general formatting 
	cap drop AREA
	g AREA = 0
	cap drop SUBDIV
	g SUBDIV = "VR"
	
	order COUNTRY SEX YEAR AREA SUBDIV VR_SOURCE NID underlying_nid
	
	if "`dropnid'" == "yes" {
		drop NID
	}
	
// saving dataset
// At this point the main part of the script is done
	saveold "`", replace

exit, clear STATA
