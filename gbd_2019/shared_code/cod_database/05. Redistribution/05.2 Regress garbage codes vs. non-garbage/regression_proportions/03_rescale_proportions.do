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
	
		// Total draws
			local total_draws 1000
	
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
		capture log using "`FILEPATH", replace


	** ****************************************************************
	** GET GBD RESOURCES
	** ****************************************************************
	// Get cap list
		insheet using "FILEPATH", comma names clear
		tempfile cap_list
		save `cap_list', replace

** **************************************************************************
** RUN PROGRAM
** **************************************************************************
	// Get data
		use "FILEPATH", clear
		
	// Cap necessary causes
		gen computed_package_id = `package'
		merge m:1 computed_package_id acause using `cap_list', keep(1 3)
		replace prop_predict = mean_cf_target_proportional if prop_predict > mean_cf_target_proportional & _m == 3 & missing != 1
		** ** ** ** ** ** ** ** ** ** ** ** ** ** **
		// Proportional caps
			** Proportional ischemic stroke under age 15
			replace prop_predict = mean_cf_target_proportional if acause == "cvd_stroke_isch" & age < 15
			
			** Proportional ischemic heart disease under age 15, EXCEPT atherosclerosis
			if `package' != 16 replace prop_predict = mean_cf_target_proportional if acause == "cvd_ihd" & age < 15
			
			** Proportional COPD under age 15
			replace prop_predict = mean_cf_target_proportional if acause == "resp_copd" & age < 15
			
		// Elimination caps
			** Eliminate bicycle and motorcycle over 70
			replace prop_predict = 0 if inlist(acause,"inj_trans_road_2wheel","inj_trans_road_pedal") & age >= 70
			replace strd_err = 0 if inlist(acause,"inj_trans_road_2wheel","inj_trans_road_pedal") & age >= 70
			
			** Eliminate IHD, ischemic stroke, COPD under 5
			replace prop_predict = 0 if inlist(acause,"cvd_ihd","cvd_stroke_isch","resp_copd") & age < 5
			replace strd_err = 0 if inlist(acause,"cvd_ihd","cvd_stroke_isch","resp_copd") & age < 5

			** Eliminate congenital over age 50
			replace prop_predict = 0 if strmatch(acause,"cong*") & age >= 45
			replace strd_err = 0 if strmatch(acause,"cong*") & age >= 45
			
			** Eliminate suicide for under 15 (should be taken care of above, but to be sure is not non-zero as artifact)
			replace prop_predict = 0 if index(acause,"inj_suicide") & age < 15
			replace strd_err = 0 if index(acause,"inj_suicide") & age < 15
			
			** Eliminate other poinsonings in alcohol RDP for 15+
			if `package' == 31 replace prop_predict = 0 if acause == "inj_poisoning_other" & age >= 15
			if `package' == 31 replace strd_err = 0 if acause == "inj_poisoning_other" & age >= 15
			
			** Eliminate other drug in drug RDPs for under 15
			if inlist(`package',27,18,19,30) replace prop_predict = 0 if acause == "mental_drug_other" & age < 15
			if inlist(`package',27,18,19,30) replace strd_err = 0 if acause == "mental_drug_other" & age < 15
			
		// Location-specific caps
			** Eliminate chagas outside of Latin America
			replace prop_predict = 0 if acause == "ntd_chagas" & super_region != "Latin America and Caribbean"
			replace strd_err = 0 if acause == "ntd_chagas" & super_region != "Latin America and Caribbean"
			replace random_effect = "none" if acause == "ntd_chagas" & super_region != "Latin America and Caribbean"
			replace random_effect = "super_region" if acause == "ntd_chagas" & super_region == "Latin America and Caribbean" & random_effect == "none"
			replace random_effect = "super_region " + random_effect if acause == "ntd_chagas" & super_region == "Latin America and Caribbean" & random_effect != "none" & !index(random_effect,"super_region")
		** ** ** ** ** ** ** ** ** ** ** ** ** ** **

	// Generate CoDCorrected proportion
		// Make 1000 draws from a normal distribution with mean = prop_predict and standard deviation = standard error
		quietly {
			noisily display "Creating `total_draws' draws..."
			** Exclude those with negative slopes
			replace strd_err = 0 if prop_predict < 0
			replace prop_predict = 0 if prop_predict < 0

			forvalues i = 1/`total_draws' {
				* gen draw_`i' = rbeta(alpha, beta) if missing == 0
				gen draw_`i' = rnormal(prop_predict, abs(strd_err)) if missing == 0
				if (round(`i'/100)==`i'/100) noisily display "     ... `i'/`total_draws'"
			}
		// Rescale all draws -- reset negatives to 0, but do not cap those that exceed 1
			noisily display "Rescaling `total_draws' draws..."
			forvalues i = 1/`total_draws' {
				replace draw_`i' = 0 if draw_`i' < 0 & missing == 0
				* replace draw_`i' = 1 if draw_`i' > 1 & missing == 0
				egen double tmp = pc(draw_`i'), prop by(global super_region region country location year sex age)
				replace draw_`i' = tmp
				drop tmp
				if (round(`i'/100)==`i'/100) noisily display "     ... `i'/`total_draws'"
			}
		}
		// Summarize draws
			egen double rescaled_mean = rowmean(draw_*)
			
		// Rescale to ensure everything adds up to either 0 or 1
			egen double wgt = pc(rescaled_mean), prop by(global super_region region country location year sex age)
			replace wgt = 0 if wgt == .
		
		// Clean up
			drop draw_*

	// Save
		keep global super_region region country location year sex age group_cause acause random_effect p_val prop_predict strd_err rescaled_mean wgt
		compress
		save "FILEPATH", replace
		
	capture log close


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
