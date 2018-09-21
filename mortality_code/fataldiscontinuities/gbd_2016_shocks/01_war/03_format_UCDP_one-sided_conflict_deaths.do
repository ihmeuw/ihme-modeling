// Author:	NAME
// Date:		3/13/2012
// Purpose:	Format UCDP one-sided conflict deaths file downloaded from http://www.pcr.uu.se/research/ucdp/datasets/ucdp_one-sided_violence_dataset/ on 3/13/2012
// 	
// Edited 2/18/2014 by NAME, to update filepaths, data directory, and country codes files
// Edited 2/25/2014 by NAME to evenly distribute deaths in multiple locations across those locations, instead of just the primary location

// set up stata
	clear all
	set more off

	// Establish directories
	// PATHdrive
	if c(os) == "Windows" {
		global prefix "PATH"
	}
	if c(os) == "Unix" {
		global prefix "/FILEPATH"
		set odbcmgr unixodbc
	}

// set directories
	// global datadir "FILEPATH"
	 global outdir "FILEPATH"

// set up country codes file
	insheet using "FILEPATH", clear
	keep if indic_cod == 1
	rename (location_name indic_cod) (countryname_ihme ihme_indic_country)
	keep countryname_ihme iso3 ihme_indic_country
	duplicates drop

	tempfile countrycodes
	save `countrycodes', replace

// bring in the UCDP database
      import delimited "FILEPATH/UCDP_ONE_SIDED_VIOLENCE_1989_2015_V1_4_2016_Y2016M10D04.CSV", clear // GBD 2016
      rename (actorid coalitioncomponents actorname year bestfatalityestimate lowfatalityestimate highfatalityestimate isgovernmentactor location gwnolocation region version) (ActorId CoalitionComponents ActorName Year BestFatalityEstimate LowFatalityEstimate HighFatalityEstimate IsGovernmentActor Location GWNOLocation Region Version)

// testing data for consistency
	replace HighFatalityEstimate=98 if BestFatalityEstimate > HighFatalityEstimate & ActorId==775
	replace BestFatalityEstimate=97 if HighFatalityEstimate==98 & ActorId==775

	assert BestFatalityEstimate >= LowFatalityEstimate
	assert BestFatalityEstimate <= HighFatalityEstimate
	assert LowFatalityEstimate <= HighFatalityEstimate


// split the deaths evenly across conflict-years that took place in multiple locations. check how many parsed variables there are
	rename GWNOLoc GWNoLoc

// drop unnecessary varibales
	drop CoalitionComponents

// replace Africa deaths where > 1 country is present using the UCDP Africa dataset; Region = 4 = Africa
	gen nid = 269698
	preserve
	keep if regexm(GWNoLoc, ",")==1 // GBD 2016: "Africa" dataset looks to have all countries now, so use that
	tempfile file
	save `file'

	// bring in Africa dataset
		use "FILEPATH", clear // GBD 2016
		destring, replace
		keep if type_of_violence==3
		rename *dset_* **
		
		rename (dbest dlow dhigh) (BestFatalityEstimate LowFatalityEstimate HighFatalityEstimate)
		rename (side_a side_a_id year gwnoloc country_name) (ActorName ActorId Year GWNoLoc Location)
		keep Year ActorId ActorName BestFatalityEstimate LowFatalityEstimate HighFatalityEstimate GWNoLoc Location
		generate Version= "Africa"
		tostring GWNoLoc, replace


		// Append UCDP and Africa dataset observations
			append using `file'

				duplicates tag Year ActorId, gen(dupID)
				duplicates tag Year ActorId Version, gen(dupVER)

			// make sure that only one UCDP non-state matches with 1 or more Africa dataset rows
					bysort Year ActorId Version: gen tag=_n
					assert tag == 1 if Version != "Africa"

				// make sure all of the UCDP observations have a duplicate
					// assert dupID > 0 if Version != "Africa" & Year <= 2010
				// 5 UCDP observations didn't have a Africa duplicate between 1990-2010
					// ActoriD 2013 is the same as 1380, so replacing it
					replace ActorId = 1380 if ActorId == 2013
					drop dupID
					duplicates tag Year ActorId, gen(dupID)
						// 1 UCDP observation doesn't have Africa dataset duplicates

				// make sure all of the Africa dataset observations have a duplicate
					// assert dupID != dupVER if Version == "Africa" & Year <= 2010
				// 25 Africa obs didn't have a UCDP duplicate between 1990 - 2010, keep tags and review these once they are appended with complete UCDP dataset
					// These 25 have very low death estimates, so I'm leaving them in
				generate tag2 = 0
				replace tag2 = 1 if dupID == dupVER & Version == "Africa"


			// This will check whether UCDP dataset Best/Low/High numbers match up with the sum of the Africa dataset numbers
				// Best/Low/High checks
				foreach var of varlist *FatalityEstimate {
					bysort ActorId Year Version: egen check = total(`var')
					gen check2 = check if Version == "Africa"
					gsort Year ActorId -Version
					bysort Year ActorId: carryforward check2, gen(`var'_checkAfrica)
					gen `var'_checkUCDP = check if Version != "Africa"
					gsort Year ActorId +Version
					bysort Year ActorId: carryforward `var'_checkUCDP, replace
					drop check2 check
					replace `var'_checkAfrica = `var' if dupID == 0 & Version != "Africa"
				}
				rename *FatalityEstimate_* **
				// tempfile preRes
				// save `preRes', replace

				// Check that the difference between the UCDP and Africa dataset death numbers is either less than 100 or less than 5%
				// these assertions fail; the code that follows is what to do about it
				// assert (abs(BestcheckAfrica - BestcheckUCDP)/BestcheckUCDP < 0.05) | (abs(BestcheckAfrica - BestcheckUCDP) < 100) if Version != "Africa"
				// assert (abs(LowcheckAfrica - LowcheckUCDP)/LowcheckUCDP < 0.05) | (abs(LowcheckAfrica - LowcheckUCDP) < 100) if Version != "Africa"
				// assert (abs(HighcheckAfrica - HighcheckUCDP)/HighcheckUCDP < 0.05) | (abs(HighcheckAfrica - HighcheckUCDP) < 100) if Version != "Africa"

				// 1 instance where there is a big discrepancy between Africa and UCDP dataset (Year: 1994; ActorId: 517)
					// For this instance, UCDP number is a lot higher. I'm keeping the UCDP number, but going to distribute the deaths based on how the Africa dataset is distributed.

					generate Best_frac = BestFatalityEstimate/ BestcheckAfrica
					generate Low_frac = LowFatalityEstimate/ LowcheckAfrica
					generate High_frac = HighFatalityEstimate/ HighcheckAfrica

					generate editbest = 1 if (abs(BestcheckAfrica - BestcheckUCDP)/BestcheckUCDP >= 0.05) & (abs(BestcheckAfrica - BestcheckUCDP) >= 100) & Version != "Africa"
					generate editlow = 1 if (abs(LowcheckAfrica - LowcheckUCDP)/LowcheckUCDP >= 0.05) & (abs(LowcheckAfrica - LowcheckUCDP) >= 100) & Version != "Africa"
					generate edithigh = 1 if (abs(HighcheckAfrica - HighcheckUCDP)/HighcheckUCDP >= 0.05) & (abs(HighcheckAfrica - HighcheckUCDP) >= 100) & Version != "Africa"

					gsort Year ActorId Version
					bysort Year ActorId: carryforward edit*, replace

					replace BestFatalityEstimate = Best_frac*BestcheckUCDP if Version == "Africa" & editbest == 1
					replace LowFatalityEstimate = Low_frac*LowcheckUCDP if Version == "Africa" & editlow == 1
					replace HighFatalityEstimate = High_frac*HighcheckUCDP if Version == "Africa"	& edithigh == 1
					drop *check* *frac edit*



			// drop duplicates
				// drop duplicates from UCDP non-state dataset
				drop if dupID > 0 & Version != "Africa"

			drop dup*
			tempfile newdata
			save `newdata'


// append to original dataset
	restore
	drop if regexm(GWNoLoc, ",")==1  // used to only drop Africa
	append using `newdata'

	// double check that there were no other duplicates between UCDP and Africa
	duplicates tag Year ActorId, gen(dupID)
	duplicates tag Year ActorId Version, gen(dupVER)
	* assert dupID == dupVER
	drop if dupID != dupVER 
	drop dup* tag* Region

// there's one case where the comma-space doesn't separate the locations; fix that by taking out all the spaces, then parse out the different locations
	replace GWNoLoc = subinstr(GWNoLoc, char(32), "", .)
	split GWNoLoc, parse( ",")
	local numberadded = `r(nvars)'

// generate an indicator variable that will tell us how many locations there are for a given conflict-dyad-year
	generate indic = 1

	forvalues varnumber = 1/ `numberadded' {
		replace indic = `varnumber' if GWNoLoc`varnumber' != ""
	}

// copy the row as many times as there are location conflicts
	expand indic

// replace the death counts with the death counts divided by the number of locations
// that is, evenly distibute the deaths across locations
	foreach deathvar of varlist BestFatalityEstimate LowFatalityEstimate HighFatalityEstimate {
		replace `deathvar' = `deathvar'/indic
	}


// replace the location id (GWNoLoc) with the correct location, given that we separated out the deaths by location in each conflict-dyad-year
	bysort ActorId Year: gen nn = _n
	qui summ nn
	local max = `r(max)'

	forvalues locationnum = 1/`max' {
		capture replace GWNoLoc = GWNoLoc`locationnum' if nn == `locationnum' & Version != "Africa"
		capture drop GWNoLoc`locationnum'
	}

	drop indic nn

// drop those observations without locations
	drop if GWNoLoc == "-99"
	destring GWNoLoc, gen(GWNOLoc)

// merge on iso3 using the data
	merge m:1 GWNOLoc using "FILEPATH"
	drop if _m == 2

// this set doesn't have south sudan; fix that
	replace iso3 = "SSD" if (GWNOLoc == 626 & _m==1)
	replace country_name = "South Sudan" if iso3 == "SSD"
	replace _m=3 if iso3 == "SSD"
	assert _m == 3
	drop _m


// the dataset above has many errors in the iso3 designations -- these are corrections
	replace iso3 = "DZA" if country_name == "Algeria"
	replace iso3 = "AGO" if country_name == "Angola"
	replace iso3 = "AUS" if country_name == "Australia"
	replace iso3 = "BGD" if country_name == "BanglUSER"
	replace iso3 = "BHR" if country_name == "Bahrain"
	replace iso3 = "BIH" if country_name == "Bosnia-Herzegovina"
	replace iso3 = "BRN" if country_name == "Brunei"
	replace iso3 = "BFA" if country_name == "Burkina Faso (Upper Volta)"
	replace iso3 = "BDI" if country_name == "Burundi"
	replace iso3 = "BTN" if country_name == "Bhutan"
	replace iso3 = "BWA" if country_name == "Botswana"
	replace iso3 = "KHM" if country_name == "Cambodia (Kampuchea)"
	replace iso3 = "CMR" if country_name == "Cameroon"
	replace iso3 = "CAF" if country_name == "Central African Republic"
	replace iso3 = "TCD" if country_name == "Chad"
	replace iso3 = "COG" if country_name == "Congo"
	replace iso3 = "COD" if country_name == "Congo, Democratic Republic of (Zaire)"
	replace iso3 = "CRI" if country_name == "Costa Rica"
	* replace iso3 = "CIV" if country_name == "Cote D�Ivoire" // Special character makes this not work, use ISO instead
	replace iso3 = "CIV" if iso3 == "CDI"
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
	replace iso3 = "KWT" if country_name == "Kuwait"
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
	replace iso3 = "SWZ" if country_name == "Swaziland"
	replace iso3 = "TJK" if country_name == "Tajikistan"
	replace iso3 = "TZA" if country_name == "Tanzania/Tanganyika"
	replace iso3 = "THA" if country_name == "Thailand"
	replace iso3 = "TGO" if country_name == "Togo"
	replace iso3 = "TTO" if country_name == "Trinidad and Tobago"
	replace iso3 = "GBR" if country_name == "United Kingdom"
	replace iso3 = "URY" if country_name == "Uruguay"
	replace iso3 = "VNM" if country_name == "Vietnam, Democratic Republic of"
	replace iso3 = "VNM" if country_name == "Vietnam, Republic of"
	replace iso3 = "YEM" if country_name == "Yemen, People's Republic of"
	replace iso3 = "ZMB" if country_name == "Zambia"
	replace iso3 = "ZWE" if country_name == "Zimbabwe (Rhodesia)"

// keep relevant variables and format
// event-type dropped here
	rename Year year
	gen dataset_ind = 2 if Version != "Africa"
	replace dataset_ind = 21 if Version == "Africa"
	keep year iso3 HighFatalityEstimate LowFatalityEstimate BestFatalityEstimate country_name dataset_ind IsGovernmentActor ActorName

// 2/26/2015: Added a cause based on IsGovernmentActor field
// 31 Mar 2017 - not assigning state-actor violence from UCDP data any more, VR only
	gen cause = ""
	replace cause = "war" if IsGovernmentActor == 1
	replace cause = "war" if regexm(ActorName, "Government")
	replace cause = "terrorism" if cause == ""

	drop IsGovernmentActor ActorName

	rename BestFatalityEstimate 	war_deaths_best
	rename LowFatalityEstimate 		war_deaths_low
	rename HighFatalityEstimate 	war_deaths_high

// Sum all deaths within a given iso3-year
	collapse (sum) war_*, by(year iso3 dataset_ind country_name cause)

// limit dataset to indicator countries
	merge m:1 iso3 using `countrycodes'
	assert _m != 1
	keep if _m == 3
	drop _m
	drop if ihme_indic != 1
	drop country_name countryname_ihme


// save final dataset
	saveold "${outdir}/UCDP_onesided_Africa.dta", replace
