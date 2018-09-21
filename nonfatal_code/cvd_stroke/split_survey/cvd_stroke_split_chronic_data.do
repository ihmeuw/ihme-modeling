// SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS

// BOILERPLATE 
	clear all
	set maxvar 10000
	set more off
  
	adopath + "FILEPATH"
  
 // PULL IN LOCATION_ID AND INCOME CATEGORY FROM BASH COMMAND
	local location "`1'"
	
// Set up locals
	local tmp_dir "FILEPATH"

// Start log
	capture log close
	log using FILEPATH/prevalence_split_`location', replace
  
// Get estimates of ischemic and hemorrhagic to split chronic stroke by
	// Ischemic (me_id=3952)
		get_draws, gbd_id_field(modelable_entity_id) gbd_id(3952) location_ids(`location') measure_ids(6 9) status(best) source(dismod) clear
	
		preserve
		keep if measure_id==9
		forvalues i = 0/999 {
			quietly generate cfr_`i' = draw_`i'/(12+draw_`i')
		}

		tempfile ischemic_cfr
		quietly save `ischemic_cfr', replace
		restore

		keep if measure_id==6
		forvalues i = 0/999 {
			quietly rename draw_`i' incidence_`i'
		}

		tempfile ischemic_incidence
		quietly save `ischemic_incidence', replace

		use `ischemic_cfr', clear
		merge 1:1 age_group_id location_id year_id sex_id using `ischemic_incidence', keep(3) nogen
			
		forvalues i = 0/999 {
			generate ischemic_`i' = incidence_`i' * (1-cfr_`i')
		}

		drop incidence_* cfr_*

		tempfile ischemic
		save `ischemic', replace
         
	// Hemorrhagic (me_id=3953)
		get_draws, gbd_id_field(modelable_entity_id) gbd_id(3953) location_ids(`location') measure_ids(6 9) status(best) source(dismod) clear
		
		preserve
        keep if measure_id==9
		forvalues i = 0/999 {
			quietly generate cfr_`i' = draw_`i'/(12+draw_`i')
		}

		tempfile cerhem_cfr
		quietly save `cerhem_cfr', replace
		restore

		keep if measure_id==6
		forvalues i = 0/999 {
			quietly rename draw_`i' incidence_`i'
		}

		tempfile cerhem_incidence
		quietly save `cerhem_incidence', replace

		use `cerhem_cfr', clear
		merge 1:1 age_group_id location_id year_id sex_id using `cerhem_incidence', keep(3) nogen
		
        forvalues i = 0/999 {
			generate cerhem_`i' = incidence_`i' * (1-cfr_`i')
		}

		drop incidence_* cfr_*

		tempfile cerhem
		save `cerhem', replace

	// Make ratio
		use `ischemic', clear

		merge 1:1 age_group_id sex_id year_id using `cerhem', keep(3) nogen
        forvalues i = 0/999 {
			gen ischemic_ratio_`i' = ischemic_`i'/(ischemic_`i' + cerhem_`i')
			gen cerhem_ratio_`i' = cerhem_`i'/(ischemic_`i' + cerhem_`i')
		}

		egen mean_ischemic = rowmean(ischemic_ratio_*)
		
		egen mean_cerhem = rowmean(cerhem_ratio_*)
		
		keep age_group_id sex_id year_id mean_ischemic mean_cerhem
		gen location_id = `location'
		tempfile ratios
		save `ratios', replace

save "FILEPATH/ratios_`location'.dta", replace
	
log close
