// NAME
// June 21, 2012
// Modified Dec 11 2015 to use cause
// Purpose: generate confidence intervals for type-specific disaster deaths



	clear all
	pause on
	set more off

		if c(os) == "Windows" {
			global prefix ""
		}
		else {
			global prefix ""
			set odbcmgr unixodbc
		}

	global datadir "FILEPATH"
	global outdir "FILEPATH"

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

	do "FILEPATH"
	create_connection_string
	local conn_string `r(conn_string)'

	do "FILEPATH"

	// Locations_ids
	//odbc load, clear dsn(prodcod) exec("SELECT location_id, ihme_loc_id as iso3 FROM shared.location_hierarchy_history WHERE location_set_version_id = 46")
	//gen l = length(iso3)
	//keep if l == 3
	//drop l
	get_location_metadata, location_set_id(35) clear
	keep if location_type == "admin0" | location_type == "nonsovereign"
	keep location_id ihme_loc_id
	rename ihme_loc_id iso3
	tempfile locations
	save `locations', replace

	// Location names
	//odbc load, clear dsn(prodcod) exec("SELECT location_id, location_name FROM shared.location_hierarchy_history WHERE location_set_version_id = 46")
	get_location_metadata, location_set_id(35) clear
	keep location_id location_ascii_name
	rename location_ascii_name location_name
	isid location_id
	tempfile location_names
	save `location_names', replace


	*************************************************************************************
	*************************************************************************************
	// bring in formatted data
	** GBD 2016 
	
	local switch = 0
	if `switch' == 1 {
		use "FILEPATH", clear
		drop if numkilled == .
		duplicates tag location_id year numkilled , gen(dup)
		preserve
			keep if dup == 0
			isid location_id year numkilled
			tempfile tech
			save `tech', replace
		restore
			keep if dup > 0
			tempfile tech_extra
			save `tech_extra', replace

		use "FILEPATH", clear
		drop if numkilled == .
		duplicates tag location_id year numkilled , gen(dup)
		preserve
			keep if dup == 0
			isid location_id year numkilled
			tempfile nat
			save `nat', replace
		restore
			keep if dup > 0
			tempfile nat_extra
			save `nat_extra', replace

		use `tech', clear
		merge 1:1 location_id year numkilled using `nat'
		* keep if _m == 1 | _m == 3
		append using `tech_extra'
		append using `nat_extra'
		drop dup
	}
	else {
		use "FILEPATH", clear
		append using "FILEPATH"
		duplicates drop
	}


	append using "FILEPATH"
		// drop Hajj from this online source, we use wiki instead
		* drop if iso3 == "SAU" & year == 2015 & cause == "Other" & source == "Online supplement 2015" 

	drop if numkilled == 0

	// TEMPORARY FIX OF ZAF SUBNATIONAL - WILL BE CORRECTED IN EPIDEMIC CODE
	replace location_id = 485 if iso3 == "ZAF_485"
	replace iso3 = "ZAF" if iso3 == "ZAF_485"
	split iso3, p("_")
	replace iso3 = iso31 if iso3 != iso31
	destring iso32, replace
	replace location_id = iso32 if location_id == . & iso32 != .
	drop iso31 iso32


	tempfile disaster
	save `disaster', replace

	// Check duplicates with VR
	append using "FILEPATH"
	replace cause = proper(cause) // Make sure we don't miss anything due to capitalization issues
	replace cause = "inj_disaster" if inlist(cause, "Earthquake", "Flood", "Flooding", "Landslide", "Natural Disaster", "Other Geophysical", "Other Hydrological", "Storm", "Volcanic Activity") ///
									| inlist(cause, "Avalanche", "Ground Movement", "Subsidence", "Tsunami", "Rockfall", "Ash fall", "Disaster") ///
									| inlist(cause, "Cyclone", "Hurricane")
	replace cause = "inj_non_disaster" if cause == "Severe Winter Conditions"
	replace cause = "inj_fires" if inlist(cause, "Fire", "Wildfire", "Forest Fire", "Land Fire (Brush, Bush, Pastur")
	replace cause = "inj_mech_other" if inlist(cause, "Collapse", "Explosion", "Other")
	replace cause = "inj_non_disaster" if inlist(cause, "Cold Wave", "Heat Wave")
	replace cause = "inj_poisoning" if inlist(cause, "Chemical Spill", "Gas Leak", "Poisoning")
	replace cause = "inj_trans_road_4wheel" if inlist(cause, "Road")
	replace cause = "inj_trans_other" if inlist(cause, "Air", "Rail", "Water", "Airplane Crash", "Train Derailment")
	replace cause = "nutrition_pem" if inlist(cause, "Drought", "Famine")
	replace cause = "inj_mech_other" if inlist(cause, "Oil Spill", "Church Collapse")
	replace cause = "diarrhea" if cause == "Cholera"
	replace cause = "ntd_dengue" if cause == "Dengue"
	replace cause = "meningitis_meningo" if cause == "Meningitis"
	replace cause = "measles" if cause == "Measles"
	replace cause = "ntd_yellowfever" if cause == "Yellow Fever"
	replace cause = lower(cause) // Make acauses lower again

	// 2.8.2016:  Drop all heat/cold wave events
	drop if cause == "inj_non_disaster"
	// 4.7.2017:  Drop all motor vehicle events
	drop if cause == "inj_trans_road_4wheel"

	// AT 1/24/17: drop garbage code disease disasters from EMDAT // 3/31/17: we still want this to get rid of lingering non-cause-fixed EMDAT entries
	drop if regexm(cause, "epidemic ")

  // Ensure we only have actual acauses
  drop if cause == "" //
	count if !regexm(cause, "inj_") & !inlist(cause, "nutrition_pem", "diarrhea", "ntd_dengue", "meningitis_meningo")
	assert `r(N)' == 0

	//
	collapse (sum) numkilled l_disaster_rate u_disaster_rate, by(iso3 location_id year cause source nid) fast
	foreach var in l_disaster_rate u_disaster_rate {
		replace `var' = . if `var' == 0
		assert `var' < 1 if `var' != .
	}


	//drop if year < 1980
	rename location_id lid
	merge m:1 iso3 using `locations', keepusing(location_id) assert(2 3) keep(3) nogen
	replace location_id = lid if lid != .
	drop lid

	// Quick data checks
	* assert nid != .
	assert source != ""
	assert location_id != .
	assert iso3 != ""
	assert numkilled != .

	// Duplicates check. Can only have 1 source per location year cause
	duplicates tag iso3 location_id year cause, gen(dup)
	gen priority = 0
	// Create a tag if VR is among duplicates for a location-year-cause group
	gen is_vr = 1 if source == "VR"
	replace is_vr = 0 if source != "VR"
	bysort iso3 location_id year cause : egen vr = max(is_vr)

	// Keep shocks data if no duplicates
	replace priority = 1 if dup == 0 & source != "VR"
	// Keep non-duplicate shocks-only causes from VR
	replace priority = 1 if dup == 0 & source == "VR" & cause == "inj_disaster"
	// Keep certain sources regardless
	replace priority = 1 if dup > 0 & source == "Hajj Wiki"

	// Drop non-duplicate CoDem run causes from VR
	drop if priority == 0 & dup == 0 & source == "VR" & cause != "inj_disaster"

	// For ALL causes keep VR duplicates from all high quality VR countries 
	// Generate high vs low quality VR indicator
	preserve
	insheet using "FILEPATH", clear
	rename ihme_loc_id iso3 
	levelsof iso3, local(isos) clean
	restore
	gen vr_quality = 0
	foreach iso of local isos {
		replace vr_quality = 1 if iso3 == "`iso'"
	}

	// High Quality VR Countries: De-duplication varies by cause
		// inj_disaster: Use VR duplicate if deaths greater than shocks duplicate
		replace priority = 1 if vr_quality == 1 & source == "VR" & cause == "inj_disaster"

		// For CoDem run causes, find the difference between the VR duplicate and the average of the surrounding VR years. Use this difference as the shocks number
		//gen indic = 1 if cause != "inj_disaster" & dup > 0
		// Get list of location-cause-years with VR duplicates. Use this list to find shock difference in VR
			preserve
			keep if cause != "inj_disaster" & dup > 0 & vr_quality == 1
			keep if source != "VR"
			keep iso3 location_id year cause numkilled
			rename numkilled deaths_shocks
			collapse (sum) deaths_shocks,by(iso3 location_id year cause) fast
			isid iso3 location_id year cause
			drop if year == 2015 & iso3 == "ITA" // AT 1/24/17 this is the only row that doesn't merge below.
			tempfile dups
			save `dups', replace

			use "FILEPATH", clear // in gbd 2016 we want to use the CoD team's VR file, not a DB query, for our VR shocks.

			merge 1:1 iso3 location_id year cause using `dups', keepusing(deaths_shocks) assert(1 3)
			gen shock = 1 if _m == 3
			replace shock = 0 if _m == 1
			drop _merge
			// Drop location-causes without any duplicates
				bysort location_id cause : egen no_shock = max(shock)
				drop if no_shock == 0
				drop no_shock
				sort iso3 location_id cause year
			// Gen shock difference for common case. T is shock year, and surrounding years are non-shock
				local vtype : type numkilled
				gen `vtype' shock_diff = numkilled - (numkilled[_n-1] + numkilled[_n+1]) / 2 if shock == 1
			// Tag end cases next. If t-1 or t+1 are from different location-cause group
				gen end_case = 0
				by iso3 location_id cause : replace end_case = 1 if ((location_id[_n-1] != location_id) | (cause[_n-1] != cause)) & shock == 1
				by iso3 location_id cause : replace end_case = 2 if ((location_id[_n+1] != location_id) | (cause[_n+1] != cause)) & shock == 1
				by iso3 location_id cause : replace end_case = 3 if ((location_id[_n-1] != location_id) | (cause[_n-1] != cause)) & ((location_id[_n+1] != location_id) | (cause[_n+1] != cause)) & shock == 3
			// Create vars for nearest non-shock year within location-cause group.
				egen group = group(iso3 location_id cause)
				bysort group : gen id = _n
				gen t_1 = 0
				gen t_2 = 0
				// First assume all adjacent years (as long as within group) are non-shock
				replace t_1 = id[_n-1] if group[_n-1] == group & shock == 1
				replace t_2 = id[_n+1] if group[_n+1] == group & shock == 1

			//For each group, we have ids for all observations. We also have shock indicator, thus we can compare a list of all ids and a list of ids that are shocks. For each shock id, find the next smallest and next largest non-shock id.
				levelsof group, local(groups)
				qui sum group
				local countdown = r(max)
				foreach g of local groups {
					quietly {
					n display `countdown'
					local --countdown
					levelsof id if group == `g' & shock == 1, local(shocks)
					foreach shock of local shocks {
						// Find next smallest non-shock id, replace t_1 = -1 if none
						sum id if group == `g' & id < `shock' & shock == 0
						if missing(r(max)) {
							replace t_1 = -1 if group == `g' & id == `shock' & shock == 1
						}
						else {
							replace t_1 = `r(max)' if group == `g' & id == `shock' & shock == 1
						}
						// Find next largest non-shock id, replace t_2 = -1 if none
						sum id if group == `g' & id > `shock' & shock == 0
						if missing(r(min)) {
							replace t_2 = -1 if group == `g' & id == `shock' & shock == 1
						}
						else {
							replace t_2 = `r(min)' if group == `g' & id == `shock' & shock == 1
						}
					}
					}
				}
			// Apply shock difference corrections
				qui sum group
				local countdown = r(max)
				foreach g of local groups {
					quietly {
					n display `countdown'
					local --countdown
					levelsof id if group == `g' & shock == 1, local(shocks)
					foreach shock of local shocks {
						levelsof t_1 if group == `g' & id == `shock', local(t_1)
						levelsof t_2 if group == `g' & id == `shock', local(t_2)

						// Case 1: There are two adjacent non-shock years
						if `t_1' != -1 & `t_2' != -1 {
							levelsof numkilled if group == `g' & id == `t_1', local(vrt_1)
							levelsof numkilled if group == `g' & id == `t_2', local(vrt_2)
							replace shock_diff = numkilled - (`vrt_1' + `vrt_2') / 2 if group == `g' & id == `shock'
						}
						// Case 2: There is only one adjacent non-shock year before, none after
						else if `t_1' != -1 & `t_2' == -1 {
							levelsof numkilled if group == `g' & id == `t_1', local(vrt_1)
							replace shock_diff = numkilled - `vrt_1' if group == `g' & id == `shock'
						}
						// Case 3: There is only one adjacent non-shock year after, none before
						else if `t_1' == -1 & `t_2' != -1 {
							levelsof numkilled if group == `g' & id == `t_2', local(vrt_2)
							replace shock_diff = numkilled - `vrt_2' if group == `g' & id == `shock'
						}
						// Case 4: There are no adjacent non-shock years
						else if `t_1' == -1 & `t_2' == -1 {
							replace shock_diff = 0 if group == `g' & id == `shock'
						}
					}
					}
				}
			// Check end cases have been properly accounted for
				assert t_1 == -1 if end_case == 1
				assert t_2 == -1 if end_case == 2

			// Remove shocks with shock diff <= 0
				replace shock = -1 if shock_diff <= 0

			// Merge back with shocks data
				keep iso3 location_id year cause shock_diff shock source
				drop if shock == 0
				rename shock_diff numkilled

				tempfile shock_diff
				save `shock_diff', replace

			restore
			// First merge to drop original duplicates (more than 1 due to multiple sources)
			merge m:1 iso3 location_id year cause using `shock_diff', keep(1) assert(1 3) nogen
			// Second merge to add shock_diffs with correct source and nid
			merge m:1 iso3 location_id year cause using `shock_diff', assert(1 2) nogen
			drop if shock == -1
			replace vr_quality = 1 if shock == 1
			replace priority = 1 if shock == 1
			replace dup = 0 if shock == 1
			drop shock



	// Low Quality VR Countries: De-duplication varies by cause
		// inj_disaster low quality VR countries, use whatever source has higher death count
		local var_type : type numkilled	// Make sure var types are the same so scientific accuracy maintained
		bysort iso3 location_id year cause : egen `var_type' high_deaths = max(numkilled)
		replace priority = 1 if cause == "inj_disaster" & vr_quality == 0 & numkilled == high_deaths
		// check process
		assert priority == 0 if cause == "inj_disaster" & vr_quality == 0 & numkilled < high_deaths

		// For CoDem run causes just look at duplicates with VR and decide if any VR points need to be outliered. Drop VR and keep shocks numbers regardless.
		replace priority = 1 if dup > 0 & vr_quality == 0 & cause != "inj_disaster" & source != "VR"
		replace priority = 0 if dup > 0 & vr_quality == 0 & cause != "inj_disaster" & source == "VR"
		// Keep Hajj Wiki over other non-VR sources
		replace priority = 0 if dup > 0 & vr_quality == 0 & cause == "inj_mech_other" & source != "Hajj Wiki" & vr == 0
		// Find potential outliers: LQVR CoDem causes with more than 50 shocks deaths, with duplicate VR data points
		// Create the 50 death minimum threshold
		sort iso3 location_id year cause source
		local vtype : type numkilled
		bysort iso3 location_id year cause : egen `vtype' death_threshold = max(numkilled) if source != "VR"
		carryforward death_threshold, replace
		bysort iso3 location_id year cause : assert death_threshold == death_threshold[1]	// Check death_threshold is constant for each group

		** BROWSE TO SEE WHAT DUPLICATES TO OUTLIER
		** br if dup > 0 & death_threshold > 50 & vr_quality == 0 & cause != "inj_disaster" & vr == 1
		// Count number of points to check in CoDVis
			count if dup > 0 & death_threshold > 50 & vr_quality == 0 & cause != "inj_disaster" & source == "VR"
			// Export potential outlier list
			preserve
			/* Keep other sources so we can compare Shocks deaths with VR deaths
			keep if dup > 0 & death_threshold > 50 & vr_quality == 0 & cause != "inj_disaster" & source == "VR"
			keep iso3 location_id year cause numkilled death_threshold
			rename death_threshold deaths_emdat
			rename numkilled deaths_vr
			*/
			keep if dup > 0 & death_threshold > 50 & vr_quality == 0 & cause != "inj_disaster" & vr == 1
			collapse (sum) numkilled, by(iso3 location_id year cause is_vr) fast

			reshape wide numkilled, i(iso3 location_id year cause) j(is_vr)

			rename numkilled0 deaths_shocks
			rename numkilled1 deaths_vr


			merge m:1 location_id using `location_names', keepusing(location_name) keep(3) assert(2 3) nogen

			sort cause iso3 year

			cap export excel using "FILEPATH", replace firstrow(var)

			restore




	drop if dup > 0 & priority == 0
	drop dup priority death_threshold high_deaths

// Quick data checks
	* assert nid != .
	assert source != ""
	assert location_id != .
	assert iso3 != ""
	assert numkilled != .

	duplicates tag iso3 location_id year cause, gen(dup)
	drop if dup > 0 & source == "EMDAT" & location_id == 43880
	drop if dup >0 & source == "VR" // there are 5 cases where the VR and EMDAT numbers are the same. Drop the VR so we don't need to deduplicate it.
	drop if dup >0 & year == 2015 & iso3 == "NGA" & source == "Online supplement 2015"
	drop if dup >0 & year == 2015 & iso3 == "SWZ" & source == "EMDAT" // this is smaller than the online supplement, drop it.
	drop if dup >0 & year == 2015 & iso3 == "EGY" & source == "Online supplement 2015" // EMDAT has it now.
	isid iso3 location_id year cause

	gen sex = "both"

	saveold "FILEPATH", replace
	saveold "FILEPATH", replace


	** Generate a file for removing VR data (selected as shocks above) from the VR file used by the Mortality/Demographics team
	** this file is used in 05.5_dedup_VR
	keep if source == "VR"
	keep iso3 location_id year cause numkilled
	rename numkilled excess_numkilled
	tempfile disaster_keep
	save `disaster_keep'
	use "FILEPATH", clear
	collapse (sum) numkilled, by(iso3 location_id year cause source nid) fast
	rename numkilled agg_numkilled
	tempfile aggnk
	save `aggnk'
	use "FILEPATH", clear
	merge m:1 iso3 location_id year cause source nid using `aggnk', assert(3) nogen
	merge m:1 iso3 location_id year cause using `disaster_keep', assert(1 3) keep(3) nogen
	// Take ratio of excess deaths determined above to age/sex aggregate, reduce all ages/sexes by that amount
	replace numkilled = numkilled - (numkilled * (excess_numkilled / agg_numkilled))
	drop excess_numkilled agg_numkilled
	saveold "FILEPATH", replace




	** end **
