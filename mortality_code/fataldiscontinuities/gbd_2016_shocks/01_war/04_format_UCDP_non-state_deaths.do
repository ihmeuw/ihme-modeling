// Author:NAME	
// Date:		3/14/2012
// Purpose:	Format UCDP non-state deaths file downloaded from http://www.pcr.uu.se/research/ucdp/datasets/ucdp_non-state_conflict_dataset_/ on 3/13/2012

// Author:NAME	
// Date:		3/13/2012
// Purpose:	Format UCDP one-sided conflict deaths file downloaded from http://www.fpcr.uu.se/research/ucdp/datasets/ucdp_one-sided_violence_dataset/ on 3/13/2012
// 			

// Edited 2/18/2014 by NAME to update filepaths, data directory, and country codes files

// STEP 1: Set directories, bring in country code file, and set up UCDP non-state dataset
// STEP 2: Keep only UCDP non-state obs from Africa and append with the Africa dataset; Double check that the aggregate death #s are the same/similar for UCDP and Africa duplicates; drop UCDP obs that have Africa duplicates


// STEP 1: Set directories, bring in country code file, and set up UCDP non-state dataset
// set up stata
	clear all
	cap restore, not
	set more off
	if c(os) == "Windows" {
		global j ""
	}
	if c(os) == "Unix" {
		global j "j"
		set odbcmgr unixodbc
	}

// set directories
	global datadir "FILEPATH"
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
	import delimited using "$datadir/UCDP_NON_STATE_CONFLICT_1989_2015_V2_5_2016_Y2016M10D04.CSV", clear // GBD 2016


// testing data for consistency
	gen tag = 1 if bestfatalityestimate < lowfatalityestimate | bestfatalityestimate > highfatalityestimate | lowfatalityestimate > highfatalityestimate
	* replace highfatalityestimate = 736 if dyadid == "2-13813" & tag == 1
	* replace bestfatalityestimate = 141 if dyadid == "2-14369" & tag == 1
	* replace highfatalityestimate = 1197 if dyadid == "2-14374" & tag == 1
	* replace lowfatalityestimate = 284 if dyadid == "2-14393" & tag == 1
	* replace highfatalityestimate = 292 if dyadid == "2-14393" & tag == 1
	* replace bestfatalityestimate = 288 if dyadid == "2-14393" & tag == 1
	* replace highfatalityestimate = 27 if dyadid == "2-14397" & tag == 1
	* replace bestfatalityestimate = 467 if dyadid == "2-378" & tag == 1
	replace bestfatalityestimate = 41 if dyadid == "2-39" & tag == 1 //  11/16/16 only this one does anything...
	drop tag


	// tests
	assert bestfatalityestimate >= lowfatalityestimate
	assert bestfatalityestimate <= highfatalityestimate
	assert lowfatalityestimate <= highfatalityestimate


// split the deaths evenly across conflict-years that took place in multiple locations. check how many parsed variables there are
	rename gwnolocation GWNoLoc

// dropping irrelevant variables
	drop sideacomponents sidebcomponents startdate startprec startdate2 startprec2 epend ependdate ependprec org

// fix dyadid variable to match Africa dataset
		destring dyadid, replace
		split dyadid, parse("-")
		drop dyadid dyadid1
		rename dyadid2 DyadID
		destring DyadID, replace


// STEP 2: Keep only UCDP non-state obs from Africa and append with the Africa dataset; Double check that the aggregate death #s are the same/similar for UCDP and Africa duplicates; drop UCDP obs that have Africa duplicates
// Africa disaggregated dataset merge
	// Make a tempfile with observations where more than 1 country was involved
				// only keeping rows from UCDP non-state if it has more than one battle location
			gen nid = 269701
			preserve
			keep if regexm(GWNoLoc, ",")==1 // & region == 4
			tempfile file
			save `file', replace

		// bring in Africa dataset and append to UCDP nonstate where more than one battle location is present
			use "$outdir/Africa.dta", clear
			destring, replace
			rename *dset_* **
			keep if type_of_violence==2
			rename (dbest dlow dhigh) (bestfatalityestimate lowfatalityestimate highfatalityestimate)
			rename (conflict_id dyad_id side_a side_a_id side_b side_b_id year country_name) (conflict_id DyadID sidea sideaid sideb sidebid year location)

			/* This section not needed for GBD 2016

			* split conflict_id, parse("-")
			* destring conflict_id2, replace
			* assert conflict_id2 == DyadID

			*/
			drop conflict_id* dyad_name conflict_name type_of_violence
			generate version= "Africa"
			// tostring GWNoLoc, replace // GWNO removed from newer datasets  11.6.2015

			append using `file'
			duplicates tag year DyadID, gen(dupID)
			duplicates tag year DyadID version, gen(dupVER)

			// make sure all of the UCDP observations have a duplicate
				* assert dupID > 0 if version != "Africa" & year <= 2010 

			// make sure all of the Africa dataset observations have a duplicate
			// assert dupID != dupVER if version == "Africa" & year <= 2010
				// 13 false -  values are too low to be significant, so leaving them as they are

			generate tag = 0
			replace tag = 1 if dupID == dupVER & version == "Africa"

			// This will check whether UCDP dataset Best/Low/High numbers match up with the sum of the Africa dataset numbers
				// Best/Low/High checks
				foreach var of varlist *fatalityestimate {
					bysort DyadID year version: egen check = total(`var')
					gen check2 = check if version == "Africa"
					gsort year DyadID -version
					bysort year DyadID: carryforward check2, gen(`var'_checkAfrica)
					gen `var'_checkUCDP = check if version != "Africa"
					gsort year DyadID +version
					bysort year DyadID: carryforward `var'_checkUCDP, replace
					drop check2 check
					replace `var'_checkAfrica = `var' if dupID == 0 & version != "Africa"
				}
				rename *fatalityestimate_* **

				// Check that the difference between the UCDP and Africa dataset death numbers is either less than 100 or less than 5%
				assert (abs(bestcheckAfrica - bestcheckUCDP)/bestcheckUCDP < 0.05) | (abs(bestcheckAfrica - bestcheckUCDP) < 100) if version != "Africa"
				assert (abs(lowcheckAfrica - lowcheckUCDP)/lowcheckUCDP < 0.05) | (abs(lowcheckAfrica - lowcheckUCDP) < 100) if version != "Africa"
				assert (abs(highcheckAfrica - highcheckUCDP)/highcheckUCDP < 0.05) | (abs(highcheckAfrica - highcheckUCDP) < 100) if version != "Africa"
					// Assertions were all true
					// If they were not, use the following code:
					* generate Best_frac = bestfatalityestimate/ bestcheckAfrica
					* generate Low_frac = lowfatalityestimate/ lowcheckAfrica
					* generate High_frac = highfatalityestimate/ highcheckAfrica

					* generate editbest = 1 if (abs(bestcheckAfrica - bestcheckUCDP)/bestcheckUCDP >= 0.05) & (abs(bestcheckAfrica - bestcheckUCDP) >= 100) & version != "Africa"
					* generate editlow = 1 if (abs(lowcheckAfrica - lowcheckUCDP)/lowcheckUCDP >= 0.05) & (abs(lowcheckAfrica - lowcheckUCDP) >= 100) & version != "Africa"
					* generate edithigh = 1 if (abs(highcheckAfrica - highcheckUCDP)/highcheckUCDP >= 0.05) & (abs(highcheckAfrica - highcheckUCDP) >= 100) & version != "Africa"

					* gsort year DyadID version
					* bysort year DyadID: carryforward edit*, replace

					* replace bestfatalityestimate = Best_frac*bestcheckUCDP if version == "Africa" & editbest == 1
					* replace lowfatalityestimate = Low_frac*lowcheckUCDP if version == "Africa" & editlow == 1
					* replace highfatalityestimate = High_frac*highcheckUCDP if version == "Africa"	& edithigh == 1
					* drop bestcheckAfrica - edithigh

				// drop duplicates from UCDP non-state dataset
				drop if dupID > 0 & version != "Africa"

			drop dup*
			tempfile newdata
			save `newdata', replace

// append to original dataset
			restore
		
			drop if regexm(GWNoLoc, ",")==1  // & region == 4
			append using `newdata'

	// double check that there were no other duplicates between UCDP and Africa
		// Drop duplicates from UCDP
			drop if year == 1991 & DyadID == 248 & version != "Africa"
			drop if year == 1996 & DyadID == 53 & version != "Africa"

		duplicates tag year DyadID, gen(dupID)
		duplicates tag year DyadID version, gen(dupVER)
		* assert dupID == dupVER	
		drop dup* tag region

// Split out the country locations when more than one is involved in the conflict and is split by a comma
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
	foreach deathvar of varlist bestfatalityestimate lowfatalityestimate highfatalityestimate {
	replace `deathvar' = `deathvar'/indic
	}

// replace the location id (GWNoLoc) with the correct location, given that we separated out the deaths by location in each conflict-dyad-year
	bysort DyadID year: gen nn = _n
	qui summ nn
	local max = `r(max)'

	// Africa points already have correct locations
	forvalues locationnum = 1/`r(max)' {
		capture replace GWNoLoc = GWNoLoc`locationnum' if nn == `locationnum' & version != "Africa"
		capture drop GWNoLoc`locationnum'
	}

// drop those observations without locations
	drop if GWNoLoc == "-99"
	destring GWNoLoc, gen(GWNOLoc)

// merge on iso3 using the data
	merge m:1 GWNOLoc using "FILEPATH"
	drop if _m == 2
	replace _m = 3 if _m == 4	// Updated missing iso3's


// this set doesn't have south sudan; fix that
	replace iso3 = "SSD" if GWNOLoc == 626 & _m==1
	replace country_name = "South Sudan" if iso3 == "SSD"
	replace _m=3 if iso3 == "SSD"

// fix serbia points - these are not included in the mapping dataset above
	replace iso3 = "SRB" if _m == 1 & GWNOLoc == 340
	replace _m = 3 if _m == 1 & iso3 == "SRB"

// replace _m = 3 if iso3 already present (GED-Africa data)
	replace _m = 3 if version == "Africa" & iso3 != ""

	* assert _m == 3 // 11/16/16: this fails, manually adding iso3s for the failed rows (they didn't have a GWNOLoc)
	replace iso3 = "ZAF" if inlist(DyadID, 310, 312) & version == "2.5-2016" & iso3 == ""
	replace iso3 = "KEN" if inlist(DyadID, 151, 110, 100, 225) & version == "2.5-2016" & iso3 == ""
	replace iso3 = "IND" if inlist(DyadID, 380, 124) & version == "2.5-2016" & iso3 == ""
	replace iso3 = "CHN_519" if DyadID == 10920 & version == "2.5-2016" & iso3 == "" // Urumqi riots 2009
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
	replace iso3 = "BWA" if country_name == "Botswana"
	replace iso3 = "KHM" if country_name == "Cambodia (Kampuchea)"
	replace iso3 = "CMR" if country_name == "Cameroon"
	replace iso3 = "CAF" if country_name == "Central African Republic"
	replace iso3 = "TCD" if country_name == "Chad"
	replace iso3 = "COG" if country_name == "Congo"
	replace iso3 = "COD" if country_name == "Congo, Democratic Republic of (Zaire)"
	replace iso3 = "CRI" if country_name == "Costa Rica"
	replace iso3 = "CIV" if country_name == "Cote D�Ivoire"
		replace iso3 = "CIV" if regexm(country_name, "Cote") & regexm(country_name, "Ivoire")
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
	replace iso3 = "KGZ" if country_name == "Kyrgyz Republic"
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
	replace iso3 = "TZA" if country_name == "Tanzania/Tanganyika"
	replace iso3 = "THA" if country_name == "Thailand"
	replace iso3 = "TGO" if country_name == "Togo"
	replace iso3 = "TTO" if country_name == "Trinidad and Tobago"
	replace iso3 = "GBR" if country_name == "United Kingdom"
	replace iso3 = "URY" if country_name == "Uruguay"
	replace iso3 = "VNM" if country_name == "Vietnam, Democratic Republic of"
	replace iso3 = "VNM" if country_name == "Vietnam, Republic of"
	replace iso3 = "YEM" if country_name == "Yemen, People's Republic of"
	replace iso3 = "ZWE" if country_name == "Zimbabwe (Rhodesia)"

// keep relevant variables and format
	rename year year
	gen dataset_ind = 3 if version == "2.5-2016"
	replace dataset_ind = 31 if version == "Africa"
	keep year iso3 highfatalityestimate lowfatalityestimate bestfatalityestimate country_name dataset_ind nid

	rename bestfatalityestimate 	war_deaths_best
	rename lowfatalityestimate 		war_deaths_low
	rename highfatalityestimate 	war_deaths_high

// Sum all deaths within a given iso3-year
	collapse (sum) war_*, by(year iso3 dataset_ind country_name nid)

// limit dataset to indicator countries
	gen iso3_subnat = ""
	replace iso3_subnat = iso3 			if regexm(iso3, "_") == 1
	replace iso3 = substr(iso3, 1, 3) 	if regexm(iso3, "_") == 1

	merge m:1 iso3 using `countrycodes'
	drop if iso3 == "" & country_name == ""
	assert _m != 1
	keep if _m == 3
	drop _m
	drop if ihme_indic != 1
	drop country_name countryname_ihme

	gen cause="war"

// save final dataset
	saveold "${outdir}/UCDP_+Africa_nonstate.dta", replace
