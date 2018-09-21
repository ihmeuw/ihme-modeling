// Author:NAME	
// Edited 04/11/2016 by NAME
// Date:          9/29/2014
// Purpose:	Format UCDP pre-1989 war deaths downloaded from http://www.prio.org/Data/Armed-Conflict/Battle-Deaths/The-Battle-Deaths-Dataset-version-30/

*******************************************************************************
** SET-UP
*******************************************************************************
	clear all
	cap restore not
	set more off

	if c(os) == "Windows" {
		global prefix ""
	}
	else if c(os) == "Unix" {
		global prefix ""
	}

// set directories
	global datadir "FILEPATH"
	global outdir "FILEPATH"	

*******************************************************************************
** IMPORTS
*******************************************************************************
	
// set up country codes file
	insheet using "FILEPATH", clear
	keep if indic_cod == 1 
	rename (location_name indic_cod) (countryname_ihme ihme_indic_country)
	keep countryname_ihme iso3 ihme_indic_country
	duplicates drop
	tempfile countrycodes
	save `countrycodes', replace

// bring in the UCDP database 
	import excel "FILEPATH", clear firstrow

*******************************************************************************
** CLEAN & SAVE
*******************************************************************************

// drop data after 1988
	drop if year > 1988 | year < 1950

	tostring gwnoloc, replace
	
		// fix gwnoloc where more than >3 countries were involved
			replace gwnoloc="651220666200" if id==55			
	
	// parse out the locations using "gwnoloc" 
	// each location id is only 3 digits, so if there is more than 1 country involved, it will be > 3 digits
	
	
	forvalues i = 1(3)18 {
		gen gwnoloc`i' = substr(gwnoloc,`i',3)
			}
			
	rename (gwnoloc4 gwnoloc7 gwnoloc10 gwnoloc13 gwnoloc16) (gwnoloc2 gwnoloc3 gwnoloc4 gwnoloc5 gwnoloc6) 
	replace gwnoloc1 = "91" if gwnoloc == "9193"
	replace gwnoloc1 = "92" if gwnoloc == "9291"
	replace gwnoloc2 = "93" if gwnoloc == "9193"
	replace gwnoloc2 = "91" if gwnoloc == "9291"

	generate location_review = 1 if gwnoloc2 != "" 
	replace location_review = 0 if gwnoloc2 == ""

	** there are conflicts with -999 best deaths... this was apparently previously replaced 
	** with the (low + high)/2. Fixed 04/11/2016
	replace bdeadbes = (bdeadlow + bdeadhig)/2 if bdeadbes==-999 
	
	tempfile complete
	save `complete', replace

	
	// data quality check
	// check war best/low/high

		replace bdeadbes = bdeadlow if bdeadlow > bdeadbes
		assert bdeadlow <= bdeadbes
		assert bdeadbes <= bdeadhig
		assert bdeadlow <= bdeadhig
	
	// generate an indicator variable that will tell us how many locations there are for a given conflict-dyad-year
		generate indic = 1
		local numberadded = 6
		forvalues varnumber = 1/ `numberadded' {
		replace indic = `varnumber' if gwnoloc`varnumber' != ""
		}
			
	// copy the row as many times as there are location conflicts
		expand indic
	
	// replace the death counts with the death counts divided by the number of locations 
	// that is, evenly distibute the deaths across locations
	// location distribution happens here
		foreach deathvar of varlist bdeadhig bdeadbes bdeadlow {
		replace `deathvar' = `deathvar'/indic
		}
		
	// replace the location id (gwnoloc) with the correct location, given that we separated out the deaths by location in each conflict-dyad-year
	bysort id year: gen nn = _n
	qui summ nn
	local max = `r(max)'
		
	forvalues locationnum = 1/`max' {
	replace gwnoloc = gwnoloc`locationnum' if nn == `locationnum'
	}
		drop gwnoloc1-nn
		
	// drop those observations without locations
		drop if gwnoloc == "-99"		
		destring gwnoloc, replace
		rename gwnoloc GWNOLoc
		
	// merge on iso3 using the data 
		merge m:1 GWNOLoc using "FILEPATH"
		drop if _m == 2 	

	// testing that merge has run properly
		assert _m == 3 			
		drop _m

	// the dataset above has many errors in the iso3 designations -- these are corrections
		replace iso3 = "DZA" if country_name == "Algeria"
		replace iso3 = "AGO" if country_name == "Angola"
		replace iso3 = "AUS" if country_name == "Australia"
		replace iso3 = "BGD" if country_name == "BanglUSER"
		replace iso3 = "BIH" if country_name == "Bosnia-Herzegovina"
		replace iso3 = "BRN" if country_name == "Brunei"
		replace iso3 = "BFA" if country_name == "Burkina Faso (Upper Volta)"
		replace iso3 = "BDI" if country_name == "Burundi"
		replace iso3 = "KHM" if country_name == "Cambodia (Kampuchea)"
		replace iso3 = "CMR" if country_name == "Cameroon"
		replace iso3 = "CAF" if country_name == "Central African Republic"
		replace iso3 = "TCD" if country_name == "Chad"
		replace iso3 = "COG" if country_name == "Congo"
		replace iso3 = "COD" if country_name == "Congo, Democratic Republic of (Zaire)"
		replace iso3 = "CRI" if country_name == "Costa Rica"
		replace iso3 = "CIV" if country_name == "Cote D’Ivoire"
		replace iso3 = "HRV" if country_name == "Croatia"
		replace iso3 = "SLV" if country_name == "El Salvador"
		replace iso3 = "GNQ" if country_name == "Equatorial Guinea"
		replace iso3 = "FRA" if country_name == "France"
		replace iso3 = "GMB" if country_name == "Gambia"
		replace iso3 = "GEO" if country_name == "Georgia"
		replace iso3 = "GTM" if country_name == "Guatemala"
		replace iso3 = "GIN" if country_name == "Guinea"
		replace iso3 = "HTI" if country_name == "Haiti"
		replace iso3 = "HND" if country_name == "Honduras"
		replace iso3 = "IDN" if country_name == "Indonesia"
		replace iso3 = "KOR" if country_name == "Korea, Republic of"
		replace iso3 = "LBN" if country_name == "Lebanon"
		replace iso3 = "LBY" if country_name == "Libya"
		replace iso3 = "LSO" if country_name == "Lesotho"
		replace iso3 = "MDG" if country_name == "Madagascar (Malagasy)"
		replace iso3 = "MYS" if country_name == "Malaysia"
		replace iso3 = "MRT" if country_name == "Mauritania"
		replace iso3 = "MDA" if country_name == "Moldova"
		replace iso3 = "MAR" if country_name == "Morocco"
		replace iso3 = "MOZ" if country_name == "Mozambique"
		replace iso3 = "MMR" if country_name == "Myanmar (Burma)"
		replace iso3 = "NPL" if country_name == "Nepal"
		replace iso3 = "NER" if country_name == "Niger"
		replace iso3 = "NGA" if country_name == "Nigeria"
		replace iso3 = "OMN" if country_name == "Oman"
		replace iso3 = "PRY" if country_name == "Paraguay"
		replace iso3 = "PHL" if country_name == "Philippines"
		replace iso3 = "ROU" if country_name == "Rumania"
		replace iso3 = "SRB" if country_name == "Serbia"
		replace iso3 = "SLE" if country_name == "Sierra Leone"
		replace iso3 = "ZAF" if country_name == "South Africa"
		replace iso3 = "ESP" if country_name == "Spain"
		replace iso3 = "LKA" if country_name == "Sri Lanka (Ceylon)"
		replace iso3 = "SDN" if country_name == "Sudan"
		replace iso3 = "TJK" if country_name == "Tajikistan"
		replace iso3 = "THA" if country_name == "Thailand"
		replace iso3 = "TGO" if country_name == "Togo"
		replace iso3 = "TTO" if country_name == "Trinidad and Tobago"
		replace iso3 = "GBR" if country_name == "United Kingdom"
		replace iso3 = "URY" if country_name == "Uruguay"
		replace iso3 = "VNM" if country_name == "Vietnam, Democratic Republic of"
		replace iso3 = "VNM" if country_name == "Vietnam, Republic of"
		replace iso3 = "YEM" if country_name == "Yemen, People's Republic of"
		replace iso3 = "ZWE" if country_name == "Zimbabwe (Rhodesia)"
		replace iso3 = "KWT" if iso3 == "KUW"
		replace iso3 = "TWN" if country_name == "Taiwan"
		replace iso3 = "TZA" if country_name == "Tanzania/Tanganyika"
		replace iso3 = "NLD" if country_name == "Netherlands"

	// keep relevant variables and format
		keep year iso3 type bdeadhig bdeadlow bdeadbes country_name CorrectedLocation 
		gen dataset_ind = 1
		rename  bdeadbes war_deaths_best
		rename  bdeadlow war_deaths_low
		rename  bdeadhig war_deaths_high
		
		rename CorrectedLocation subdiv
	 
	// Sum all deaths within a given iso3-year
		collapse (sum) war_*, by(year iso3 subdiv dataset_ind )

	// limit dataset to indicator countries
		merge m:1 iso3 using `countrycodes'
		drop if _m == 2	
		drop _m 
		drop countryname_ihme
		
		gen cause = "war"
	
	 // save dataset
		saveold "$outdir/Battles_1950-1989_deaths_split.dta", replace
	
	
	
	
	
	
	
	

		
