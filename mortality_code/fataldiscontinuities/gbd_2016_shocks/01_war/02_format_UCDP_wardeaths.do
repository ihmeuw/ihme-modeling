// Author:	NAME
// Date:		3/13/2012
// Purpose:	Format UCDP war deaths file downloaded from http://www.pcr.uu.se/research/ucdp/datasets/ucdp_battle-related_deaths_dataset/ on 3/13/2012 by KTL
// 			GBD revisions are in process and there is a need to update the war death estimates - this database was previously updated by Alison Levin-Rector
// Edited 2/18/2014 by NAME, to update filepaths, data directory, and country codes files
// Edited 2/25/2014 by NAME to evenly distribute deaths in multiple locations across those locations, instead of just the primary location
// Edited by NAME to replace deaths in Africa where >1 country is involved with the UCDP Africa georeferenced event dataset
// set up stata


*******************************************************************************
** SET-UP
*******************************************************************************
	clear all
	cap restore not
	set more off

// Set the time
	local date = c(current_date)
	local today = date("`date'", "DMY")
	local year = year(`today')
	local month = month(`today')
	local day = day(`today')
	local time = c(current_time)
	local time : subinstr local time ":" "", all
	local length : length local month
	if `length' == 1 local month = "0`month'"
	local length : length local day
	if `length' == 1 local day = "0`day'"
	global date = "`year'_`month'_`day'"
	global timestamp = "${date}_`time'"

// Establish directories
	// J:drive
	if c(os) == "Windows" {
		global prefix ""
	}
	if c(os) == "Unix" {
		global prefix ""
		set odbcmgr unixodbc
	}

// set directories
	global datadir "FILEPATH"
	global outdir "FILEPATH"

*******************************************************************************
** METADATA IMPORTS
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
	// GBD 2016 <-- current
	import delimited "FILEPATH", clear    


*******************************************************************************
** DATE CLEANING
*******************************************************************************

	// edit the numbers that don't follow high<=best<=low
	//AT: future change, just sort the tuple (low, best, high) and re-assign it...
	replace bdhigh = bdlow if bdlow > bdhigh
	replace bdbest = bdlow if bdlow > bdbest
	replace bdbest = bdhigh if bdbest > bdhigh
	// Peru-Sendero Luminoso 1993
	replace bdhigh = 1981 if bdlow==579			

// check war best/low/high; data consistency
	assert bdlow <= bdbest
	assert bdbest <= bdhigh
	assert bdlow <= bdhigh

//  variable edits
	drop sidea2nd sideb2nd incompatibility typeofconflict gwnoa gwnoa2nd gwnob gwnob2nd 
	rename locationinc location
	keep year conflictid dyadid location battlelocation gwnoloc gwnobattle region version bdbest bdlow bdhigh
	// for agreement with 'Africa' dataset
	tostring version, replace 

// generate an indicator variable that will tell us how many locations there are for a given conflict-dyad-year
	
	replace dyadid = subinstr(dyadid, char(32), "", .)
	split dyadid, parse( ",")
	local numberadded = `r(nvars)'
	* destring dyadid*, replace
	** split dyad-ids evenly across all countries involved
	generate dyad_idx = 1
	forvalues varnumber = 1/ `numberadded' {
		replace dyad_idx = `varnumber' if dyadid`varnumber' != ""
	}
	** create a new row for each copy of the dyad_id
	expand dyad_idx
	** distribute deaths evenly over each dyad
	foreach deathvar of varlist bdhigh bdlow bdbest {
		replace `deathvar' = `deathvar'/dyad_idx
	}
	// replace the dyad id with the correct one, given that we separated out the deaths by dyad_id in each conflict-year
	bysort conflictid year: gen ndyad = _n
	qui summ ndyad
	local max = `r(max)'
	forvalues locationnum = 1/`max' {
		capture replace dyadid = dyadid`locationnum' if ndyad == `locationnum' & version != "Africa"
	}


// replace Africa deaths where > 1 country is present using the UCDP Africa dataset; Region = 4 = Africa
	preserve
	keep if (regexm(gwnoloc, ",")==1 | regexm(gwnobattle, ",")==1) 
	tempfile file
	save `file'

// bring in Africa dataset
		 // GBD 2016
		use "FILEPATH", clear
		// specifically for type_of_violence, but we'll see if it helps with other things
		destring, replace 
		keep if type_of_violence == 1
		rename (dlow dbest dhigh) (bdlow bdbest bdhigh)
		rename *_* **
		rename *dset* **
		rename *_* **
		rename countryname location
		keep year conflictid dyadid bdlow bdbest bdhigh gwnoloc location
		generate version= "Africa"
		tostring gwnoloc, replace
		tostring dyadid, replace



		// Append UCDP and Africa dataset observations
			append using `file'

				duplicates tag year conflictid dyadid, gen(dupID)
				duplicates tag year conflictid dyadid version, gen(dupVER)

				// make sure all of the UCDP observations have a duplicate
					assert dupVER == 0 if version != "Africa" & year <= 2010
					// assert dupID > 0 if version != "Africa" & year <= 2010
						// 15 UCDP observations didn't have a Africa duplicate between 1990-2010; Only 4 of these observations actually had a battle location in Africa
							// Leaving these observations as they are since it isn't a conflictid/dyadid error

				// make sure all of the Africa dataset observations have a duplicate
					// assert dupID != dupVER if version == "Africa" & year <= 2010
						// 10 Africa obs didn't have a UCDP duplicate between 1990 - 2010, keep tags and review these once they are appended with complete UCDP dataset
					generate tag = 0
					replace tag = 1 if dupID == dupVER & version == "Africa"

			// This will check whether UCDP dataset Best/Low/High numbers match up with the sum of the Africa dataset numbers
				// Best/Low/High checks
				foreach var of varlist bd* {
					bysort conflictid dyadid year version: egen check = total(`var')
					gen check2 = check if version == "Africa"
					gsort year conflictid dyadid -version
					bysort year conflictid dyadid: carryforward check2, gen(`var'_checkAfrica)
					gen `var'_checkUCDP = check if version != "Africa"
					gsort year conflictid dyadid +version
					bysort year conflictid dyadid: carryforward `var'_checkUCDP, replace
					drop check2 check
					replace `var'_checkAfrica = `var' if dupID == 0 & version != "Africa"
				}
				rename (bdbest_* bdlow_* bdhigh_*) (Best* Low* High*)

				// 3 instance where there is a big discrepancy between Africa and UCDP dataset (Year: 1994; ActorId: 517)
					// For this instance, UCDP number is a lot higher. I'm keeping the UCDP number, but going to distribute the deaths based on how the Africa dataset is distributed.

					generate Best_frac = bdbest/ BestcheckAfrica
					generate Low_frac = bdlow/ LowcheckAfrica
					generate High_frac = bdhigh/ HighcheckAfrica

					generate editbest = 1 if (abs(BestcheckAfrica - BestcheckUCDP)/BestcheckUCDP >= 0.05) & (abs(BestcheckAfrica - BestcheckUCDP) >= 100) & version != "Africa"
					generate editlow = 1 if (abs(LowcheckAfrica - LowcheckUCDP)/LowcheckUCDP >= 0.05) & (abs(LowcheckAfrica - LowcheckUCDP) >= 100) & version != "Africa"
					generate edithigh = 1 if (abs(HighcheckAfrica - HighcheckUCDP)/HighcheckUCDP >= 0.05) & (abs(HighcheckAfrica - HighcheckUCDP) >= 100) & version != "Africa"

					gsort year conflictid dyadid version
					bysort year conflictid dyadid: carryforward edit*, replace

					** this was incorrectly attributing the deaths of Kosovar Albanians to Albania; they are considered Serbian nationals for GBD.
					replace bdbest = Best_frac*BestcheckUCDP if version == "Africa" & editbest == 1 & location != "Albania" & location != "Armenia" & location != "Djibouti"
					replace bdlow = Low_frac*LowcheckUCDP if version == "Africa" & editlow == 1 & location != "Albania" & location != "Armenia" & location != "Djibouti"
					replace bdhigh = High_frac*HighcheckUCDP if version == "Africa"	& edithigh == 1 & location != "Albania" & location != "Armenia" & location != "Djibouti"
					drop BestcheckAfrica - edithigh

			// drop duplicates
			** not a duplicate:
			replace dupID = 0 if year == 1999 & conflictid == 218 & dyadid == "295"
				// drop duplicates from UCDP non-state dataset
				drop if dupID > 0 & version != "Africa"


			drop dup*
			tempfile newdata
			save `newdata'

			// save the ids for a later test
			keep conflictid
			duplicates drop
			tempfile new_ids
			save `new_ids', replace

// replace original with better africa multi-state detail
	restore
	// first assert that no ids are dropped entirely by the replace
	preserve
		// keep the subset of data that will be dropped
		keep if (regexm(gwnoloc, ",")==1 | regexm(gwnobattle, ",")==1)
		keep conflictid
		duplicates drop
		tempfile old_ids_to_drop
		save `old_ids_to_drop', replace
		// use a merge asser to make sure that all ids in 'old_ids_to_drop'
		// 	are in 'new_ids'
		// no conflict ids in the original should be missing from
		// 	the new (none missing in "master")
		merge 1:1 conflictid using `new_ids', assert(2 3)
		// if this passes, we are safe to continue
	restore

	drop if (regexm(gwnoloc, ",")==1 | regexm(gwnobattle, ",")==1)
	append using `newdata'

// double check that there were no other duplicates between UCDP and Africa
	duplicates tag year conflictid dyadid, gen(dupID)
	duplicates tag year conflictid dyadid version, gen(dupVER)
	// assert dupID == dupVER
	// 4 pairs of duplicates between UCDP and Africa -- keeping UCDP since the numbers were about the same and there was no issue about what country the deaths should be attributed to
	drop if dupID != dupVER & version == "Africa" & dupID == 1 & location != "Albania"

	drop dup* tag region


// there's one case where the comma-space doesn't separate the locations; fix that by taking out all the spaces, then parse out the different locations
	replace gwnoloc = subinstr(gwnoloc, char(32), "", .)
	split gwnoloc, parse( ",")
	local numberadded = `r(nvars)'

// generate an indicator variable that will tell us how many locations there are for a given conflict-dyad-year
	generate indic = 1

	forvalues varnumber = 1/ `numberadded' {
		replace indic = `varnumber' if gwnoloc`varnumber' != ""
	}
// copy the row as many times as there are location conflicts
	expand indic

// replace the death counts with the death counts divided by the number of locations
// that is, evenly distibute the deaths across locations
	foreach deathvar of varlist bdhigh bdlow bdbest {
		replace `deathvar' = `deathvar'/indic
	}


// replace the location id (gwnoloc) with the correct location, given that we separated out the deaths by location in each conflict-dyad-year
	bysort conflictid dyadid year: gen nn = _n
	qui summ nn
	local max = `r(max)'
	** quick fix ** AT 3/27/17
	**local max = 4 // This drops two battle-related sets of datapoints where nn == 5 | nn == 6. One is related to SAU, PHL, IRQ, and KWT.
	*************************
	forvalues locationnum = 1/`max' {
		capture replace gwnoloc = gwnoloc`locationnum' if nn == `locationnum' & version != "Africa"
	}

	drop gwnoloc1-nn

	replace gwnoloc = gwnobattle if (location == "Colombia" | location == "Ethiopia") & gwnoloc == ""
	replace gwnoloc = "345" if location == "Serbia (Yugoslavia)" & gwnoloc == ""

// drop those observations without locations
	drop if gwnoloc == "-99"
	destring gwnoloc, replace
	rename gwnoloc GWNOLoc

// merge on iso3 using the data
	merge m:1 GWNOLoc using "FILEPATH"
	drop if _m == 2

// this set doesn't have south sudan; fix that
	replace iso3 = "SSD" if regexm(location, "South Sudan")==1
	replace country_name = "South Sudan" if iso3 == "SSD"
	replace iso3 = "SSD" if conflictid == 113 & GWNOLoc == .
	replace _m=3 if iso3 == "SSD"

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
	replace iso3="KWT" if iso3=="KUW"

// keep relevant variables and format
	gen dataset_ind = 1 if version != "Africa"
	replace dataset_ind = 11 if version == "Africa"
	keep year iso3 bdhigh bdlow bdbest country_name dataset_ind

	rename bdbest war_deaths_best
	rename bdlow war_deaths_low
	rename bdhigh war_deaths_high

// Sum all deaths within a given iso3-year
	collapse (sum) war_*, by(year iso3 dataset_ind country_name)

// limit dataset to indicator countries
	merge m:1 iso3 using `countrycodes'
	drop if _m == 2
	drop _m
	drop if ihme_indic != 1
	drop country_name countryname_ihme

// fix consistency issues:
	replace war_deaths_best = war_deaths_low if war_deaths_best < war_deaths_low // Turkey 1998
	replace war_deaths_high = war_deaths_low if war_deaths_high < war_deaths_low // Ethiopia 2000
	replace war_deaths_high = war_deaths_best if year == 1998 & iso3 == "ERI"

// consistency check
	gen tag = 0
	replace tag = 1 if  war_deaths_best < war_deaths_low
	replace tag = 1 if war_deaths_high < war_deaths_low
	replace tag = 1 if war_deaths_high < war_deaths_best

	egen max_deaths = rowmax(war_deaths_best war_deaths_high war_deaths_low)
	egen min_deaths = rowmin(war_deaths_best war_deaths_high war_deaths_low)
	gen best_deaths = 0
	foreach var of varlist war_deaths_low war_deaths_best war_deaths_high {
		replace best_deaths = `var' if `var' != min_deaths & `var' != max_deaths
	}

	replace war_deaths_high = max_deaths if tag == 1
	replace war_deaths_best = best_deaths if tag == 1
	replace war_deaths_low = min_deaths if tag == 1

	replace war_deaths_best = 48147 if iso3 == "ETH" & year == 2000 & war_deaths_best == 0

	assert war_deaths_best >= war_deaths_low
	assert war_deaths_high >= war_deaths_low
	assert war_deaths_high >= war_deaths_best

// 2/26/2015: Make a few slight changes to the final output to show that this is cause "war" and to conform the deaths variable to formatting style
gen cause="war"

// 12/1/16: add nid for UCDP_battles
gen nid = 269700

// save
saveold "FILEPATH/UCDP_battles.dta", replace
