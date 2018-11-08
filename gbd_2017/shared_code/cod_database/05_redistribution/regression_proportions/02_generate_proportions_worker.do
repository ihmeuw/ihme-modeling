		// Set application preferences
			// Clear memory and set memory and variable limits
				clear all
				set mem 10G
				set maxvar 32000

			// Set to run all selected code without pausing
				set more off

			// Set graph output color scheme
				set scheme s1color

			// Install package that allows us to export regression coefficients
				ssc install parmest

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
			
		// Analytical group id
			local group__id = "`5'"
	
		// Today
			local today "`6'"
	
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
		log using "`log_folder'/regress_`group__id'.log", replace

** ****************************************************************
** RUN PROGRAM
** ****************************************************************
	// Get data
		use "FILEPATH", clear
		
	// What age will be the reference for the regressions
		foreach age in 75 45 60 {
			count if group_age == `age' & cf_garbage > 0
			if `r(N)' > 0 local age_ref `age'
		}
		di "REFERENCE AGE: `age_ref'"
		
	// Get whether there is any garbage
		recast double cf_garbage cf_target
		count if cf_garbage > 0
		local has_garbage = `r(N)'

	// Set ages to integer
		if "`scheme'" == "2_sexes" {
			replace age = 91 if age == 0
			replace age = 93 if age > 0.009 & age < 0.011
			replace age = 94 if age > 0.09 & age < 0.11
		}
		else if "`scheme'" == "2_sexes_6_ages" replace age = group_age

	// Regress
		display in red "** ******************************************************************* **"
		display in red "   Regressing: `target'"
		display in red "** ******************************************************************* **"

		quietly {
		if `has_garbage' > 0 {
			local regression_failed = "no"
			capture noisily mixed cf_target c.cf_garbage##ib`age_ref'.age || super_region: cf_garbage || region: cf_garbage, iterate(20) // || country: cf_garbage
			if !_rc & e(converged)==1 {
				noisily di ""
				noisily di "Super region converged!"
				noisily di ""
				local random_effects = "omega_super_region gamma_super_region omega_region gamma_region" //  omega_country gamma_country
			}
			else {
				clear all
				use "FILEPATH"
				recast double cf_garbage cf_target
				if "`scheme'" == "2_sexes" {
					replace age = 91 if age == 0
					replace age = 93 if age > 0.009 & age < 0.011
					replace age = 94 if age > 0.09 & age < 0.11
				}
				else if "`scheme'" == "2_sexes_6_ages" replace age = group_age
				capture noisily mixed cf_target c.cf_garbage##ib`age_ref'.age || super_region: || region: cf_garbage, iterate(20) // || country: cf_garbage
				if !_rc & e(converged)==1 {
					noisily di ""
					noisily di "Region converged!"
					noisily di ""
					local random_effects = "gamma_super_region omega_region gamma_region" //  omega_country gamma_country
				}
				else {
						clear all
						use "FILEPATH"
						recast double cf_garbage cf_target
						if "`scheme'" == "2_sexes" {
							replace age = 91 if age == 0
							replace age = 93 if age > 0.009 & age < 0.011
							replace age = 94 if age > 0.09 & age < 0.11
						}
						else if "`scheme'" == "2_sexes_6_ages" replace age = group_age
						capture noisily mixed cf_target c.cf_garbage##ib`age_ref'.age
						if !_rc & e(converged)==1 {
							noisily di ""
							noisily di "No random slopes, all geographies failed to converge"
							noisily di ""
							local random_effects = ""
						}
						else {
							noisily di ""
							noisily di "No random slopes, all geographies failed to converge"
							noisily di ""
							local regression_failed = "yes"
						}
					** }
				}
			}
		}
		else local regression_failed = "yes"
		}
		// **************************************************************************************************************************************************************************************
	
	// Predict
		if "`regression_failed'" == "no" {
		// FIXED EFFECTS
		// Extract the coefficients and load into data frame
			parmest, saving("FILEPATH", replace)
			preserve
				use "FILEPATH", clear
				keep if index(parm,"age#c.cf_garbage")
				replace parm = subinstr(parm,".age#c.cf_garbage","",.)
				keep parm estimate stderr
				rename parm age
				rename estimate beta_cf_garbage_age_inter
				rename stderr beta_cf_garbage_age_inter_se
				destring age, replace
				tempfile interactions
				save `interactions', replace
			restore
			merge m:1 age using `interactions', assert(1 3) nogen
			replace beta_cf_garbage_age_inter = 0 if beta_cf_garbage_age_inter == .
			replace beta_cf_garbage_age_inter_se = 0 if beta_cf_garbage_age_inter_se == .

		// Garbage fraction is the first covariate, its coefficient is the first cell in the matrix
			gen beta_cf_garbage = _b[cf_garbage]

		// Get the standard error of that slope
			gen beta_cf_garbage_se = _se[cf_garbage]

		// compute p value for diagnostics
			gen p_val = 2*(1-normal(abs(_b[cf_garbage]/_se[cf_garbage])))

		// RANDOM EFFECTS
		if "`random_effects'" != "" {
			// Predict random effects, as well as their standard errors
				predict `random_effects', reff
				local random_effects_ses = subinstr("`random_effects'"," ","_se ",.)
				if "`random_effects_ses'" != "" local random_effects_ses "`random_effects_ses'_se"
				predict `random_effects_ses', reses

				if !index("`random_effects'","omega_super_region") & "`random_effects'" != "" {
					preserve
						collapse (mean) omega_region omega_region_se, by(group_* super_region)
						renpfix omega_region omega_super_region
						tempfile super_region_reffs
						save `super_region_reffs', replace
					restore
					merge m:1 group_* super_region using `super_region_reffs', assert(3) nogen
				}
			}
			else if  "`random_effects'" == "" {
				foreach genvar in omega_super_region omega_super_region_se omega_region omega_region_se { // omega_country omega_country_se
					gen `genvar' = 0
				} 
			}
		}
		else {
			foreach genvar in p_val beta_cf_garbage beta_cf_garbage_se beta_cf_garbage_age_inter beta_cf_garbage_age_inter_se omega_super_region omega_super_region_se omega_region omega_region_se { // omega_country omega_country_se
				gen `genvar' = 0
			}
		}
	
	// Get average CF of target for capped causes
		preserve
			collapse (mean) cf_target_proportional, by(global) fast
			rename cf_target_proportional mean_cf_target_proportional
			tempfile targ_mean
			save `targ_mean', replace
		restore

	// Keep what we need
		keep group_* age global super_region region country p_val beta_cf_garbage* omega_super_region* omega_region* // omega_country*
		duplicates drop
		merge m:1 global using `targ_mean', assert(3) nogen
	
	// Save
		save "FILEPATH", replace

	capture log close

	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
