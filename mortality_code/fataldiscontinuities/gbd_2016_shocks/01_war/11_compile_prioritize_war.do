// Aggregate raw death counts from three war databases together and create per capita datasets


*******************************************************************************
** SET-UP
*******************************************************************************

// set up stata
	clear
	set mem 500m
	set more off
	pause on
	capture log close
	capture restore, not

	// Set the timestamp
	local date = c(current_date)
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
	local date = "`year'_`month'_`day'"
	local timestamp = "`date'_`time'"

	if c(os) == "Windows" {
		global prefix "PATH"
	}
	else if c(os) == "Unix" {
		global prefix "/FILEPATH"
	}

// GBD resources
	do "FILEPATH"

// set globals
	global datadir "FILEPATH"

// Subnational
	// Use mortality output flat file to stay consistent with methodology in 08_format_2014_online_death_sources.do
	use "FILEPATH", clear
		rename year_id year
		keep if sex_id == 3 & (age_group_id >= 2 & age_group_id <= 21)
		gen keep = 0
		foreach iso in BRA CHN GBR IDN IND JPN KEN MEX SWE SAU USA ZAF {
			replace keep = 1 if regexm(ihme_loc_id, "`iso'")
		}
		replace keep = 0 if ihme_loc_id == "CHN_44533"
		drop if keep == 0
		drop sex* age* parent_id keep source location_id location_name
		collapse (sum) pop, by(ihme_loc_id year) fast
		drop if length(ihme_loc_id)==3
		rename pop pop_
		// Drop India state, only use urban rural
			gen iso_length = length(ihme_loc_id)
			drop if regexm(ihme_loc_id, "IND_") & iso_length == 8
			drop iso_length
		// Drop England
			drop if ihme_loc_id == "GBR_4749"

		reshape wide pop_, i(year) j(ihme_loc_id) string
		foreach iso in BRA CHN GBR IDN IND JPN KEN MEX SWE SAU USA ZAF {
			looUSER pop_`iso'
			local varlist `r(varlist)'
			egen pop_`iso'_tot = rowtotal(pop_`iso'_*)
			foreach var of local varlist {
				local stub = subinstr("`var'", "pop_", "", 1)
				gen weight_`stub' = `var' / pop_`iso'_tot
			}
		}
		drop *tot pop_*
		reshape long weight_, i(year) j(ihme_loc_id) string
		split ihme_loc_id, parse("_")
		rename ihme_loc_id1 iso3
		rename ihme_loc_id2 location_id
		rename weight_ weight
		drop if location_id == "tot"
		destring location_id, replace
		tempfile sub_weight
		save `sub_weight', replace

*******************************
// DATA COMPILING
// bring in all individual datasets // see ("06_combine_UCDP_wardeaths_versions.do")
	// 1. UCDP
	use "$datadir/Battles_1950-1989_deaths_split.dta", clear
	gen nid = 224834
	append using "$datadir/UCDP_+Africa_nonstate.dta"
	replace nid = 231049 if nid == .
	append using "$datadir/UCDP_onesided_Africa.dta"
	replace nid = 231046 if nid == .
	append using "$datadir/UCDP_battles.dta"
	replace nid = 224832 if nid == .

	gen source = "UCDP"
	replace dataset_ind = 1
	drop if year == .


	append using "$datadir/IISS_alldeaths.dta"
	* keep if inlist(iso3,"CHN", "SRB", "MLI", "NPL", "SYR")
	drop if iso3== "SYR" & year > 2010
	drop if iso3 == "PSE" & year != 2013
	drop if iso3 == "RUS" & year == 2004
	drop if iso3 == "COD" & year == 2005

	replace nid = 93128 if nid == .
	replace source = "IISS" if source == ""

	append using "$datadir/online_supplement_2015.dta"
	replace nid = NID if NID != .
	drop if nid == .
	drop NID
	replace source = "Online supplement" if source == ""

	append using "$datadir/robert_strauss_center_data.dta"

	// SUBNATIONALS
	replace iso3 = iso3 + "_482" if subdiv == "Eastern Cape" & iso3=="ZAF"
	replace iso3 = iso3 + "_483" if subdiv == "Free State" & iso3=="ZAF"
	replace iso3 = iso3 + "_484" if subdiv == "Gauteng" & iso3=="ZAF"
	replace iso3 = iso3 + "_485" if subdiv == "KwaZulu-Natal" & iso3=="ZAF"
	replace iso3 = iso3 + "_485" if subdiv == "KwaZulu-Nata" & iso3=="ZAF"
	replace iso3 = iso3 + "_486" if subdiv == "Limpopo" & iso3=="ZAF"
	replace iso3 = iso3 + "_488" if subdiv == "North West" & iso3=="ZAF"
	replace iso3 = iso3 + "_488" if subdiv == "North-West" & iso3=="ZAF"
	replace iso3 = iso3 + "_489" if subdiv == "Northern Cape" & iso3=="ZAF"
	replace iso3 = iso3 + "_490" if subdiv == "Western Cape" & iso3=="ZAF"
	replace iso3 = iso3 + "_519" if subdiv == "Xinjiang" & iso3=="CHN"
	replace iso3 = iso3 + "_433" if subdiv == "Northern Ireland" & iso3=="GBR"
	replace iso3 = iso3 + "_35646" if subdiv == "Nairobi" & iso3=="KEN"
	replace iso3 = iso3 + "_433" if subdiv == "Northern Ireland" & iso3=="KEN"	
	// 2013 Nairobi Westgate shooting
	replace iso3 = iso3 + "_" + "35646" if iso3 == "KEN" & year == 2013 & cause == "terrorism"

	// drop 9/11 from UCDP, better from VR
	drop if iso3 == "USA" & year == 2001 & source == "UCDP" & nid == 231046

	// drop wierd spike in 2003 from UCDP (probably this is an aggregate of all years and/or countries in the Iraq conflict)
	drop if iso3 == "USA" & year == 2003 & source == "UCDP" & nid == 269700



	// 4.8.2016: first fix any missing codes in war_deaths_low/high
	foreach est in low high {
		replace war_deaths_`est' = . if war_deaths_`est' < 0
	}
	collapse (sum)  war_deaths_best war_deaths_low war_deaths_high , by(year iso3 subdiv cause dataset source nid) fast

	drop if war_deaths_best <= 10	// NAME consult 12/16/2015

	split iso3, parse("_")
	drop iso3
	rename iso31 iso3
	rename iso32 location_id
	destring location_id, replace



	// SPLIT REMAINING COUNTRIES THAT NEED SUBNATIONAL ESTIMATION
	gen split = 0
	replace split = 1 if year >= 1970 & (inlist(iso3, "BRA", "CHN", "GBR", "IDN", "IND", "JPN", "KEN") | inlist(iso3, "MEX", "SWE", "SAU", "USA", "ZAF")) & location_id == .

	tempfile all_data
	save `all_data', replace

	keep if split == 1

	collapse (sum) war_deaths_*, by(iso3 year cause dataset source nid location_id) fast
	preserve

	levelsof nid, local(nids) clean
	levelsof cause, local(causes)
	// there are three causes: war, terrorism, LI
	* egen cause_num = group(cause)
	* sort cause
	* egen cause_num = group(cause)
	* summarize cause_num, meanonly
	* exit(73)
	foreach nid of local nids {
		foreach cause of local causes {
			keep if nid == `nid'
			keep if cause == "`cause'"
			merge 1:m iso3 year using `sub_weight' // can't merge using cause as the sub_weight data doesn't have a cause variable.
			assert _m != 1
			drop if _m == 2
			drop _m

		foreach est in low best high {
			replace war_deaths_`est' = war_deaths_`est' * weight
		}
		replace nid = `nid' if nid == .
		tempfile split_`nid'_`cause'
		save `split_`nid'_`cause'', replace
		restore, preserve
		}

	}
	restore, not
	clear
	foreach nid of local nids {
		foreach cause of local causes {
			append using `split_`nid'_`cause''
		}
	}

	tempfile sub_split
	save `sub_split', replace

	// APPEND SPLIT DATA BACK
	use `all_data', clear
	gen iso3_l = length(iso3)
	replace location_id = . if iso3=="IND" & iso3_l == 4
	replace location_id = . if iso3=="GBR" & location_id == 4749
	drop iso3_l
	drop if split == 1
	// No population pre 1970 subnationally
	drop if year < 1970 & (inlist(iso3, "BRA", "CHN", "GBR", "IDN", "IND", "JPN", "KEN") | inlist(iso3, "MEX", "SWE", "SAU", "USA", "ZAF")) & location_id == .

	append using `sub_split'

	replace iso3 = ihme_loc_id if ihme_loc_id != ""
	replace iso3 = iso3 + "_" + string(location_id) if location_id != .


**************************************************************************************
** DUPLICATES CHECK
**************************************************************************************
	* to get the dataset indicator number for the combined sources of Arab countries, get the max of the indicators, then add one
	qui summ dataset
	local combined_datasetindicator = `r(max)'+1

	drop weight ihme_loc_id location_id

	append using "$datadir/VR_shocks_war_from_COD_VR_file.dta"
		replace cause = "war" if inlist(cause, "inj_war", "inj_war_war")
		replace iso3 = iso3 + "_" + string(location_id) if source == "VR" & (inlist(iso3, "BRA", "CHN", "GBR", "IDN", "IND", "JPN", "KEN") | inlist(iso3, "MEX", "SWE", "ZAF", "SAU", "USA"))
		replace dataset_ind = 99 if source == "VR"

*************************************************
// MANUAL CHANGES
// drop MEX drug violence - these get estimated elsewhere
	drop if year > 2000 & iso3 == "MEX" & source != "VR"

// drop AUS from UCDP, incorrectly attributing 2000 deaths in 2003 (Iraq war)
	drop if iso3 == "AUS" & source == "UCDP"

	drop if nid != 163892 & year == 2011 & iso3 == "LBY" & cause == "war"	// keep NATO
	drop if nid != 93128 & year == 2012 & iso3 == "LBY"	 & cause == "war"	// keep IISS
	drop if nid != 225048 & year == 2015 & iso3 == "SYR" & cause == "war"	// keep 2015 online supplementation
	//drop if nid != 666 & year == 2015 & iso3 == "YEM" & cause == "war"
	drop if nid != 667 & year == 2015 & inlist(iso3, "NGA", "LBY", "EGY", "SDN", "SSD") & cause == "war"

	// Use UCDP for 2015 in Nigeria, not online source
	drop if iso3 == "NGA" & year == 2015 & source == "Online supplement"
	
	//collapse (sum) war_deaths_*, fast by(iso3 subdiv year cause nid source dataset_ind)

// label the datasets
	label def datasets 1 "UCDP" 4 "IISS all deaths" 99 "VR" 100 "Online 2014-2015 supplement" 101 "Robert Strauss Center" `combined_datasetindicator' "combined sources for Arab countries", modify
	label val dataset_ind datasets
	sort iso3 year cause
	by iso3 year cause: egen max = max(war_deaths_best)
	replace max = floor(max)


**************************************************
// ORDER OF PREFERANCE SORTING
// drop duplicates - order of preference is:
	// 1. VR if it's the highest
	// 2. UCDP
	// 3. IISS
	// 4. VR
	// 5. Robert Strauss
 	// 6. Online supplement 2014

	gen priority = 1 if war_deaths_best >= max & dataset_ind == 99
	** drop if dataset_ind == 99 & priority != 1

	duplicates tag iso3 year cause, gen(dup)
	replace priority = 2 if dataset_ind == 1
	replace priority = 3 if dataset_ind == 4
	replace priority = 4 if dataset_ind == 99 & priority != 1
	replace priority = 5 if dataset_ind == 101
	replace priority = 6 if dataset_ind == 100
	replace priority = 7 if dataset_ind == `combined_datasetindicator'


	// for SYR 2011-2013, we don't trust UCDP or IISS; the estimates for those years are too low
	qui summ priority
	local syr_incorrect_data = `r(max)'+1
	//  4.8.2016: Update this, after consult with NAME. IISS has higher deaths than UCDP for these two years, look more accurate when compared with deaths in 2013-2015
	replace priority = `syr_incorrect_data' if iso3 == "SYR" & inlist(year, 2011,2012) & source == "UCDP"
	//replace priority = `syr_incorrect_data' if iso3 == "SYR" & inlist(year, 2011, 2012, 2013) & inlist(dataset_ind, 1, 4)

	sort iso3 year cause priority
	//by iso3 year cause: drop if dup >0 & _n != 1
	//  4.8.2016: The above drop doesn't work with NID, now we have multiple NIDs per source so there isn't always one obs per source
	by iso3 year cause : egen keep = min(priority)
	keep if priority == keep

// create per capita war deaths by 1 year blocks
	capture drop source
	decode dataset_ind, gen(source)
	drop dataset_ind dup
	rename iso3 ihme_loc_id
	collapse (sum) war_deaths_*, by(ihme_loc_id year cause nid source) fast
	* replace nid = -1 if source == "VR"
	isid ihme_loc_id year cause nid
	//drop if year < 1950


	generate sex = "both"

// merge on population
	// No population data pre 1970
	//drop if year < 1970

	// Fix some old location ids
	replace ihme_loc_id = "CHN_361" if ihme_loc_id == "MAC"
	replace ihme_loc_id = "SRB" if ihme_loc_id == "XKX"

	** remove duplicate RSC data for GHA 1994
	drop if source =="Robert Strauss Center" & ihme_loc_id == "GHA" & (year == 1994 | year == 2001) & cause == "terrorism"

	collapse (sum) war_deaths_*, by(year cause nid ihme_loc_id source sex) fast
	drop if war_deaths_best == 0


	saveold "$datadir/archive/war_compiled_prioritized_`timestamp'", replace
	saveold "$datadir/war_compiled_prioritized.dta", replace
