** ***********************************************************************
** Description: Formats noncod data into a format so it can be combined with noncod
** this script only exists to conform with historically used data formats.
** ***********************************************************************


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

import delimited using "", clear
keep ihme_loc_id location_id
tempfile countrymaster
save `countrymaster'

import delimited "", clear
tempfile source_type_ids
save `source_type_ids', replace

*******************************************************************
******* format NON-COD VR ************
*******************************************************************
use "", clear

foreach var of varlist deaths2-deaths389 {
		replace `var' = 0 if `var' == .
}


// format deaths into standard morality data structure: the data are in COD format 2
//  generating terminal age group - although all of this data is theoretically formatting in COD format 2
// there are often cases when deaths235 does not have any deaths -- Need to determine terminal agegroup manually
	gen terminal = .
	local age_ids = "235 32 31 160 30 20 19 18 17 16 15 14 13 12 11 10 9 8 7 6 34 238 389 388 2 3"
	foreach age of local age_ids {
		replace terminal = `age' if terminal == . & deaths`age' != 0
	}

	//there are cases that males and females from the same source have different terminal ages
	//we'd want to use the maximum terminal age of males and females
	egen max_terminal = max(terminal), by(iso3 location_id year source NID underlying_nid source_type_id)
	replace terminal = max_terminal
	drop max_terminal


	** As of now, smallest terminal is 60+ - make 60+, 65+, etc. cols
	local terminal_ages_new = "231 154 26 234 21 294"
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

merge m:1 location_id using `countrymaster', keep(3) nogen

//rename columns
rename ihme_loc_id COUNTRY
rename location_id LOCATION
rename sex SEX
rename year YEAR
rename source VR_SOURCE
gen FOOTNOTE = " "

// 5-20 continuous, then have to append 28,30,31,32,235 manually
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
	forvalues k = 60(5)90 {
		replace DATUM`k'plus = 0 if DATUM`k'plus == .
		local kplus = `k' + 4
		replace DATUM`k'to`kplus' = 0 if DATUM`k'plus != 0
	}
	
	lookfor DATUM
	foreach var of varlist `r(varlist)' {
		summ `var'
		if `r(N)' == 0 {
			drop `var'
		}
	}

	drop deaths*
	cap assert DATUM95to99 == . | DATUM95to99 == 0
	cap drop DATUM95to99

	cap assert DATUMUNK !=.
	cap drop DATUMUNK

	// Drop POL/NOR WHO data, all 0, favor subnational aggregation
	drop if iso3=="NOR" & YEAR >= 1986 & YEAR <= 2014 & VR_SOURCE=="WHO" 
	drop if iso3=="POL" & YEAR >= 1988 & YEAR <= 1996 & VR_SOURCE == "WHO"

	// Drop CHN_PROV_CENSUS for loc 44533/year2000, all 0
	drop if iso3=="CHN" & YEAR==2000 & LOCATION==44533 & VR_SOURCE=="CHN_PROV_CENSUS"

	// Prioritize duplicated US VR
	drop if LOCATION==102 & inlist(NID, 287600, 233896) & inrange(YEAR, 1959, 1979) // "WHO" US VR national
	
	// Drop WHO POL 2018, NOR 2015-16, GBR 2016, GBR 2018, all 0 except under 5, using country specific
	drop if iso3=="POL" & YEAR==2018 & VR_SOURCE=="WHO"
	drop if iso3=="NOR" & inrange(YEAR,2015,2016) & VR_SOURCE=="WHO"
	drop if iso3=="GBR" & YEAR==2018 & VR_SOURCE=="WHO"
	
	// Drop GBR 2018 nid 433133, only 0 
	drop if LOCATION==95 & YEAR==2018 & NID==433133
	drop if LOCATION==95 & YEAR==2016 & NID==318260
	
	// Drop POL 2020 from Eurostat, only 0?, use other country specific
	drop if iso3=="POL" & YEAR==2020 & NID==494016	

	// data check 
	duplicates tag COUNTRY LOCATION YEAR SEX source_type_id, g(dup)
	
	saveold "", replace
	assert dup == 0
	drop dup terminal
	
	cap gen DATUMUNK = .
	cap drop DATUMTOT
	egen DATUMTOT = rowtotal(DATUM*)
	assert DATUMTOT != .

	
	assert inlist(SEX, 1, 2)
	// add in both sexes combined
	preserve
		lookfor DATUM
		foreach var of varlist `r(varlist)' {
			replace `var' = -10000000 if `var' == .
		}
		collapse (sum) DATUM*, by(COUNTRY YEAR VR_SOURCE FOOTNOTE NID underlying_nid source_type_id)
		gen SEX = 0

		lookfor DATUM
		foreach var of varlist `r(varlist)' {
			replace `var' = . if `var' < 0
		}	

		tempfile both
		save `both', replace
	restore

	append using `both'
	
	gen AREA = .
	assert source_type_id != .

	merge m:1 source_type_id using `source_type_ids', keep(1 3) nogen
	rename source_type SUBDIV
	

	// source_type_id is dropped here
	replace SUBDIV = "DSP>2003" if NID == 338606 & VR_SOURCE == "CHN_DSP"
	drop LOCATION iso3
	sort COUNTRY YEAR SEX AREA SUBDIV VR_SOURCE NID underlying_nid
	saveold "", replace
	
exit, clear STATA
