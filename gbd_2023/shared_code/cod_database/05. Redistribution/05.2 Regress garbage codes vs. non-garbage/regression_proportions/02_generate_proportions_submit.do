
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
	
		// Code folder
			local code_folder "FILEPATH"
	
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
			local output_folder "`input_folder'"
			
			
	** ****************************************************************
	** CREATE LOG
	** ****************************************************************
		local log_folder "FILEPATH"
		capture mkdir "`log_folder'"
		capture log close
		log using "FILEPATH", replace


	** ****************************************************************
	** LOCAL CHECK
	** ****************************************************************


	** ****************************************************************
	** GET GBD RESOURCES
	** ****************************************************************		
		// Get target and garbage codes
			// Get data
				use "FILEPATH", clear
			// Keep only the package we need
				keep if computed_package_id == `package'
			// Rename variables
				rename package_set_name codebook_name
				rename code cause
			// Create group_cause
				egen group_cause = group(acause)
				replace group_cause = group_cause - 1
			// Make sure decimal points are removed
				replace cause = subinstr(cause,".","",.)
			// Save
				compress
				tempfile rdp_codes
				save `rdp_codes', replace
				
		// Get group codes for the end
			 keep group_cause acause
			 keep if group_cause > 0
			 duplicates drop
			 tempfile group_cause_names
			 save `group_cause_names', replace
				
		// Get schema data			
			// Get full
				capture use "FILEPATH", clear
				if _rc {
					sleep 5000
					use "FILEPATH", clear
				}
				preserve
					keep ihme_loc_id year sex age group_* scheme_*
					tempfile scheme_data
					save `scheme_data', replace
				restore
			// Get slim
				keep global super_region region country age group_* scheme_*
				if "`scheme'" == "2_sexes_6_ages" {
					drop age
					gen age = group_age
				}
				drop if country == ""
				duplicates drop
				tempfile scheme_data_slim
				save `scheme_data_slim', replace
			
		// Make square data
			// Start with scheme data
				use `scheme_data_slim', clear
				gen s = 1
			// Save
				tempfile square_data
				save `square_data', replace
			// Merge on group data
				use `group_cause_names', clear
				gen s = 1
				joinby s using `square_data'
			// Save
				drop s
				compress
				tempfile square_data
				save `square_data', replace
				
		// Get location hierarchy
			use "FILEPATH", clear
			tempfile location_hierarchy
			save `location_hierarchy', replace

** ****************************************************************
** RUN PROGRAM
** ****************************************************************
	// Get prepped data
		capture use "FILEPATH", clear
		if _rc {
			sleep 5000
			use "FILEPATH", clear
		}

	// Keep only the rows and columns that we need
		// Keep only codes from rdp
			replace cause = subinstr(cause,".","",.)
			merge m:1 cause codebook_name using `rdp_codes', keep(3) nogen
	// Collapse to country level if geographic level is >= country
		display "Collapsing by country"
		collapse (sum) metric, by(global super_region region country year sex cause codebook_name orig_codebook age code_type computed_package_id acause group_cause) fast
		foreach lt in global super_region region country subnational_level1 subnational_level2 {
			capture gen `lt' = ""
		}
		merge m:1 global super_region region country subnational_level1 subnational_level2 using `location_hierarchy', keep(1 3) assert(2 3) nogen
	// Merge on location, year, sex, and age analysis groupings
		joinby ihme_loc_id year sex age using `scheme_data', unmatched(master)
		count if _m != 3
		if `r(N)' > 0 {
			noisily display "There are unmerged rows with the scheme template!"
			BREAK
		}
		drop _merge
	// Manual corrections pre-regression
		// Female cancer package
		if `package' == 4 | `package' == 5 {
			replace metric = 0 if sex == 1
		}
		// Male cancer package
		if `package' == 6 {
			replace metric = 0 if sex == 2
		}
		// Restrict other suicide under 15 (poisonings, hanging, drowning, explosions, steam, knife, blunt, jump, crash)
		if (`package' >= 25 & `package' <= 37) | (`package' == 41) | (`package' >= 43 & `package' <= 47) {
			replace metric = 0 if age < 15 & acause == "inj_suicide_other"
		}
		// Restrict congenital heart over 50 (left and right heart failure)
		if inlist(`package', 14, 19) {
			replace metric = 0 if acause == "cong_heart" & age >= 50
		}
		// Restrict interstitial lung disease (right heart failure)
		if inlist(`package', 19) {
			replace metric = 0 if acause == "resp_interstitial" & age < 15
		}
	// Measure universe by country-age-sex-year, drop data points if equal to zero for a given group
		egen double universe = total(metric), by(year sex age ihme_loc_id)
		drop if universe == 0
		drop universe
	// Save deaths by garbage/target for each region
		preserve
			replace acause = "garbage" if acause == "_gc"
			replace acause = "target" if acause != "garbage"
			collapse (sum) metric, by(super_region region age sex acause)
			save "FILEPATH", replace
		restore
	// Calculate cause fractions for the universe of garbage + target codes
		egen double cf_target = pc(metric), by(year sex age ihme_loc_id) prop

		gen temp = 0
		replace temp = cf_target if group_cause == 0
		egen cf_garbage = max(temp), by(year sex age ihme_loc_id)
		drop if group_cause == 0
	// Get proportion of targets without garbage for capping
		preserve
			collapse (sum) metric, by(sex group_age group_cause)
			egen double cf_target_proportional = pc(metric), by(sex group_age) prop
			tempfile cftp
			save `cftp', replace
		restore
		merge m:1 sex group_age group_cause using `cftp', assert(3) nogen

		keep year sex age group_* ihme_loc_id global super_region region country cf_*
		
	// Loop through different location, year, sex, age, and cause groups
		// Create analytical group ids
			if "`scheme'" == "2_sexes" gen group__id = string(group_location)+"_"+string(group_year)+"_"+string(group_sex)+"_"+string(group_age)+"_"+string(group_cause)
			else if "`scheme'" == "2_sexes_6_ages" gen group__id = string(group_location)+"_"+string(group_year)+"_"+string(group_sex)+"_"+string(group_cause) // Run all ages together
			save "FILEPATH", replace
			levelsof(group__id), local(analysis_groups) clean
			keep year sex age group_* ihme_loc_id global super_region region country cf_*
		// Save and submit jobs
		preserve
			foreach group__id of local analysis_groups {
				display "Submitting `group__id'"
				keep if group__id == "`group__id'"
				capture rm "FILEPATH"
				save "FILEPATH", replace
				!FILEPATH  -P ADDRESS -pe multi_slot 5 -l mem_free=10g -N "RDP_regress_`package'_`scheme'_`group__id'" "FILEPATH" "FILEPATH" "`username' `scheme' `package' `input_data_name' `group__id' `today'"
				restore, preserve
			}
		restore
** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **
** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **
		// Check for completion
			clear
			foreach group__id of local analysis_groups {
				display "Checking for `group__id'"
				local checkfile "FILEPATH"
				capture confirm file "`checkfile'"
				while _rc {
					sleep 30000
					capture confirm file "`checkfile'"
				}
				display "Found!"
				append using "`checkfile'"
			}
			tempfile reg_prod
			save `reg_prod', replace
			keep global p_val beta_cf_garbage beta_cf_garbage_se beta_cf_garbage_age_inter beta_cf_garbage_age_inter_se mean_cf_target_proportional group_* age
			duplicates drop
			tempfile global_reg_prod
			save `global_reg_prod', replace	
			foreach geo in super_region region { // country
				use `geo' omega_`geo' omega_`geo'_se group_* age using `reg_prod', clear
				duplicates drop
				tempfile `geo'_reg_prod
				save ``geo'_reg_prod', replace //''
			}
	// Format
		// Merge on scheme information
			use `square_data', clear
			if "`scheme'" == "2_sexes" {
				replace age = 91 if age == 0
				replace age = 93 if age > 0.009 & age < 0.011
				replace age = 94 if age > 0.09 & age < 0.11
			}
			foreach geo in global super_region region { // country
				joinby `geo' group_location group_year group_sex group_age age group_cause using ``geo'_reg_prod', unmatched(master) //''
				drop _merge
			}
			foreach v in location year sex {
				rename scheme_`v' `v'
			}
			drop scheme_age
			recast double age
			replace age = 0 if age == 91
			replace age = 0.01 if age == 93
			replace age = 0.1 if age == 94
		// Load person years
			merge m:1 country using "FILEPATH", assert(1 3) nogen
			replace person_years = 0 if person_years == .
		// Supress all random effects for Andean Latin America
			replace omega_region = 0 if region == "Andean Latin America"
			replace omega_region_se = 0 if region == "Andean Latin America"
		// Produce uncertainty and calculate random effect using significant levels and person-years
			gen random_slope = 0
			gen random_slope_se = 0
			gen random_effect = "none"
			foreach geo in super_region region { // country
				** Generate UIs (if standard error is missing, replace with "mean", so it is not used)
				replace omega_`geo'_se = omega_`geo' if omega_`geo'_se == .
				gen `geo'_upper = omega_`geo' + abs(omega_`geo'_se)*1.96
				gen `geo'_lower = omega_`geo' - abs(omega_`geo'_se)*1.96
				** In order for country effect to be used, must have total person years more than 100 million
				if "`geo'" == "country" local py_limit = "& person_years > 100000000"
				else local py_limit = ""
				replace random_slope = random_slope + omega_`geo' if (`geo'_upper < 0 | `geo'_lower > 0) `py_limit'
				replace random_slope_se = random_slope_se + omega_`geo'_se if (`geo'_upper < 0 | `geo'_lower > 0) `py_limit'
				replace random_effect = random_effect + " `geo'" if (`geo'_upper < 0 | `geo'_lower > 0) `py_limit'
			}
			replace random_slope = random_slope
			replace random_effect = subinstr(random_effect,"none ","",.)
		// Compute the amount of garbage that can be explained by the current target (use inverse), and its standard error
			egen double prop_predict = rowtotal(beta_cf_garbage beta_cf_garbage_age_inter random_slope)
			replace prop_predict = -1*prop_predict
			egen double strd_err = rowtotal(beta_cf_garbage_se beta_cf_garbage_age_inter_se random_slope_se)
		// Missing flag
			gen missing = 0
			replace missing = 1 if prop_predict == .
		// Remove duplicates from the square values
			egen missing_min = min(missing), by(global super_region region country sex age group_cause)
			drop if missing_min == 0 & missing == 1
		// Keep only what we need
			keep global super_region region country location year sex age group_cause acause mean_cf_target_proportional random_effect p_val beta_cf_garbage beta_cf_garbage_se random_slope random_slope_se omega_super_region omega_super_region_se omega_region omega_region_se prop_predict strd_err missing // omega_country omega_country_se
			duplicates drop
			isid country year sex age group_cause acause
			order global super_region region country location year sex age group_cause acause mean_cf_target_proportional random_effect p_val beta_cf_garbage beta_cf_garbage_se random_slope random_slope_se omega_super_region omega_super_region_se omega_region omega_region_se prop_predict strd_err missing // omega_country omega_country_se
			sort global super_region region country location year sex age group_cause
	// Save
		save "FILEPATH", replace

	capture log close
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
