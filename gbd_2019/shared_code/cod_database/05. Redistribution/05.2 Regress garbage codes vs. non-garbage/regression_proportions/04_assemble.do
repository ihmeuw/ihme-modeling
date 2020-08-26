		// Set application preferences
			// Clear memory and set memory and variable limits
				clear all
				set mem 10G
				set maxvar 32000

			// Set to run all selected code without pausing
				set more off

			// Set graph output color scheme
				set scheme s1color

			// Define J drive (data) for cluster (UNIX) and Windows (Windows)
				if c(os) == "Unix" {
					global prefix "/home/j"
					global prefixh "FILEPATH"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "J:"
					global prefixh "H:"
				}

			// Set up PDF maker
				do "FILEPATH"

			// Get date
				local today = date(c(current_date), "DMY")
				local year = year(`today')
				local month = string(month(`today'),"%02.0f")
				local day = string(day(`today'),"%02.0f")
				local today = "`year'_`month'_`day'"


	** ****************************************************************
	** DEFINE LOCALS
	** ****************************************************************
		set seed 19870214
		
		// Database connection
			local dsn "ADDRESS"
	
		// User
			local username "`1'"
		
		// Scheme
			local scheme "`2'"
			
		// Package
			local package `3'
			
		// Input data file
			local input_data_name "`4'"
			
		// Today
			local today "`5'"

		// Input folder
			local input_folder "FILEPATH"
			
		// Temp folder
			capture mkdir "FILEPATH"
			capture mkdir "FILEPATH"
			capture mkdir "FILEPATH"
			capture mkdir "FILEPATH"
			capture mkdir "FILEPATH"
			capture mkdir "FILEPATH"
			capture mkdir "FILEPATH"
			capture mkdir "FILEPATH"
			local temp_folder "FILEPATH"
		
		// Output folder
			capture mkdir "FILEPATH"
			capture mkdir "FILEPATH"
			capture mkdir "FILEPATH"
			local output_folder "FILEPATH"
			
	** ****************************************************************
	** GET GBD RESOURCES
	** ****************************************************************
	// Get country-iso3 map
		use "FILEPATH", clear
		drop if country == ""
		gen iso3 = substr(ihme_loc_id,1,3)
		keep iso3 country
		duplicates drop
		drop if inlist(country,"Puerto Rico","Virgin Islands, U.S.","American Samoa","Guam","Northern Mariana Islands") & iso3 == "USA"
		tempfile isos
		save `isos', replace

	// Get data
			use "FILEPATH", clear

	// Manual adjustents
		egen wgts = total(wgt), by(country sex age)
		if inlist(`package',39,40) {

			* Homicide with Firearm
			replace wgt = 0.950 if age < 15 & country == "South Africa" & acause == "inj_homicide_gun"
			replace wgt = 0.860 if age >= 15 & sex == "Female" & country == "South Africa" & acause == "inj_homicide_gun"
			replace wgt = 0.869 if age >= 15 & sex == "Male" & country == "South Africa" & acause == "inj_homicide_gun"
			* Self-harm with Firearm
			replace wgt = 0.132 if age >= 15 & sex == "Female" & country == "South Africa" & acause == "inj_suicide_firearm"
			replace wgt = 0.123 if age >= 15 & sex == "Male" & country == "South Africa" & acause == "inj_suicide_firearm"
			* Unintentional with Firearm
			replace wgt = 0.050 if age < 15 & country == "South Africa" & acause == "inj_mech_gun"
			replace wgt = 0.008 if age >= 15 & country == "South Africa" & acause == "inj_mech_gun"
		}
		if `package' == 4 {
		** Add other cancer as target for the uterine cancer package under age 15
			preserve
				keep if age < 15 & sex == "Female" & acause == "neo_uterine_cancer"
				replace acause = "neo_other_cancer"
				replace wgt = 1
				tempfile other
				save `other', replace
			restore
			append using `other'
		}
		if `package' == 8 {
		** Assign other cancer as target if no target exists for endocrine package
			replace wgt = 1 if age < 15 & acause == "neo_other_cancer" & wgts == 0
		}
		if `package' == 16 {
			levelsof group_cause if acause == "cvd_stroke_isch", local(gc) c
			replace group_cause = `gc' if acause == "cvd_htn"
			replace acause = "cvd_stroke_isch" if acause == "cvd_htn"
		}

	// For packages with no targets, set to proportional
		drop wgts
		gen pw_dist = 1
		egen prop_wgt = pc(pw_dist), prop by(country sex age)
		egen wgts = total(wgt), by(country sex age)
		replace wgt = prop_wgt if wgts == 0
		
	// Replace ages as string ranges if we have bands
		if "`scheme'" == "2_sexes_6_ages" {
			gen scheme_age = "<15" if age == 0
			replace scheme_age = "15-29" if age == 15
			replace scheme_age = "30-44" if age == 30
			replace scheme_age = "45-59" if age == 45
			replace scheme_age = "60-74" if age == 60
			replace scheme_age = "75+" if age == 75
			drop age
			rename scheme_age age
		}
		
	// Collapse to level we need
		collapse (sum) wgt, by(country sex age group_cause acause) fast
		
		drop wgts pw_dist prop_wgt
		egen wgts = total(wgt), by(country sex age)
		assert (wgts < 1.001 & wgts > 0.999)
		drop wgts

	// Reformat
		foreach var of varlist * {
			capture count if `var' == .
			if _rc count if `var' == ""
			if `r(N)' > 0 {
				di "`var' is missing `r(N)' rows"
				BREAK
			}
		}
		order country sex age group_cause acause wgt
		sort country sex age group_cause
		
	// Save
		compress
		save "FILEPATH", replace
		save "FILEPATH", replace
		
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
