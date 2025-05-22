
// setting up stata
	clear all
	set more off
	set mem 500m
	cap restore, not

if c(os)=="Windows" {
	local prefix="FILEPATH"
	}
if c(os)=="Unix" {
	local prefix="FILEPATH"
	}

global new_run_id = ""

local run_folder = "FILEPATH"
local input_dir = "FILEPATH"
local output_dir = "FILEPATH"

local all_cause_dir = "FILEPATH"

// full locations for GBD
	import delimited using "FILEPATH", clear
	keep location_name ihme_loc_id location_type location_id
	gen gbd_country_iso3 = substr(ihme_loc_id, 1, 3) if inlist(location_type, "admin1", "admin2")
	keep location_name ihme_loc_id gbd_country_iso3 location_id
	duplicates drop
	tempfile countrycodes
	save `countrycodes', replace

cd "FILEPATH"

local dropnid = "no"

use "FILEPATH", clear
replace location_id = 152 if iso3 == "SAU"
replace NID = 287600 if NID == .

merge m:1 location_id using `countrycodes', keep(3)
drop gbd_country_iso3


// drop if sex is not male or female.  drop years that are not right (9999)
drop if sex == 9 | sex == 0
drop if year == 9999

	gen gbd_country_iso3 = iso3 if location_id != .

		replace iso3 = "HKG" if location_id == 354
		replace iso3 = "MAC" if location_id == 361
		replace gbd_country_iso3 = "" if inlist(iso3, "HKG", "MAC")
		replace location_id = . if inlist(iso3, "HKG", "MAC")
		replace ihme_loc_id = "HKG" if iso3 == "HKG"
		replace ihme_loc_id = "MAC" if iso3 == "MAC"
		// PRI is labelled as PRI and USA_385
		replace ihme_loc_id = "PRI" if location_id == 385
		replace gbd_country = "" if ihme_loc_id == "PRI"

		// only use ihme_loc_id to avoid errors with subnational locations
		drop iso3
		order ihme

// Drop before duplication check
drop if ihme_loc_id == "GBR" & year == 2021 & NID == 521161

// collapse across subdivisions
	preserve
		sort ihme_loc_id sex year source estimate_stage_id
		by ihme_loc_id sex year source estimate_stage_id: generate nobs = _N
		keep if nobs > 1
		save "FILEPATH", replace
	restore
	isid ihme_loc_id sex year source estimate_stage_id
	collapse (sum) deaths*, by(ihme_loc_id gbd_country_iso3 sex year source NID underlying_nid estimate_stage_id)


** *******************************************************************************************************

// 1950 point in japan
drop if ihme_loc_id == "JPN" & year == 1950

// Drop points where there is a larger than 1% difference with the WHO databank
drop if ihme_loc_id == "BRA" & year == 2009
drop if ihme_loc_id == "COL" & inlist(year, 1981, 1988, 1995, 1996, 2002, 2003, 2004, 2009)
drop if ihme_loc_id == "MEX" & year >= 1979 & year <= 2012
drop if ihme_loc_id == "MNG" & year >= 2004 & year <= 2008
drop if ihme_loc_id == "PER" & inlist(year, 2002, 2003, 2004, 2005)
drop if ihme_loc_id == "SAU" & year == 2009

	** Romania
	drop if ihme_loc_id == "ROU" & source == "ICD7A" & year <=1968
	** Honduras
	drop if ihme_loc_id == "HND" & source == "ICD7A" & year == 1966

	** Argentina 1966 and 1967
	drop if ihme_loc_id == "ARG" & source == "ICD7A" & inlist(year, 1966, 1967)
	** Israel
	drop if ihme_loc_id == "ISR" & inlist(source, "ICD7A", "ICD8A") & year <=1974 & year >= 1954
	** Malaysia
	drop if ihme_loc_id == "MYS" & (year >=1997 & year <=2008)
	** portugal
	drop if ihme_loc_id == "PRT" & source == "ICD10" & inlist(year, 2005, 2006)
	** UK regions
	drop if ihme_loc_id == "GBR" & year == 1995
	** BRA
	drop if ihme_loc_id == "BRA" & year < 2012

	** UKR 2013
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

// format deaths into standard morality data structure
	gen terminal = 235

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
	gen DATUMpostneonatal = .
	gen DATUM1to1 = deaths238
	gen DATUM2to4 = deaths34
	gen DATUM1to4 = .
	gen DATUM80to84 = deaths30
	gen DATUM85to89 = deaths31
	gen DATUM90to94 = deaths32
	gen DATUM95plus = deaths235

	// Add pnn, age group 5 if missing
	replace DATUMpostneonatal = deaths388 + deaths389 if deaths388 != . & deaths389 != .
	replace DATUM1to4 = deaths238 + deaths34 if deaths238 != . & deaths34 != .

	// handle 60 plus, 65 plus, ... 90 plus
	forvalues j= 60(5)90 {
		cap gen DATUM`j'plus = .
	}

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

	drop if _m == 2
	drop _m

	gen VR_SOURCE = "WHO_causesofdeath"
	rename ihme_loc_id COUNTRY
	rename sex SEX
	rename year YEAR
	g FOOTNOTE = ""

	// data check
	sort COUNTRY YEAR SEX estimate_stage_id source NID
    quietly by COUNTRY YEAR SEX estimate_stage_id:  gen dup = cond(_N==1,0,_n)
	preserve
	keep if dup>0
	saveold "FILEPATH", replace
	restore
	drop if dup>1

	drop dup

	drop source terminal location_name gbd_country_iso3 location_id

	// add in unknown deaths and total deaths
	cap g DATUMUNK = .
	cap drop DATUMTOT
	egen DATUMTOT = rowtotal(DATUM*)
	// Need to remove aggregate overlapping age groups from total
	replace DATUMTOT = DATUMTOT - (DATUMpostneonatal + DATUM1to4)
	assert DATUMTOT != .

	assert inlist(SEX, 1, 2)
	// add in both sexes combined
	preserve
		lookfor DATUM
		foreach var of varlist `r(varlist)' {
			replace `var' = -10000000 if `var' == .
		}
		collapse (sum) DATUM*, by(COUNTRY YEAR VR_SOURCE FOOTNOTE NID underlying_nid estimate_stage_id)
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

	order COUNTRY SEX YEAR AREA SUBDIV VR_SOURCE NID underlying_nid estimate_stage_id

	if "`dropnid'" == "yes" {
		drop NID
	}

// saving dataset
	saveold "FILEPATH", replace

exit, clear STATA
