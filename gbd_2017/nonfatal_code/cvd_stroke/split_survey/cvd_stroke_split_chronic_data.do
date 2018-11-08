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
	log using "FILEPATH", replace

// Get estimates of ischemic and hemorrhagic to split chronic stroke by
	// Ischemic (me_id=3952)
		get_draws, gbd_id_type(modelable_entity_id) gbd_id(9310) location_id(`location') measure_id(6 9) status(best) source(epi) clear

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

	// Intracerebral hemorrhage (me_id=3953)
		get_draws, gbd_id_type(modelable_entity_id) gbd_id(9311) location_id(`location') measure_id(6 9) status(best) source(epi) clear

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

	// Subarachnoid hemorrhage (me_id=18730)
		get_draws, gbd_id_type(modelable_entity_id) gbd_id(18731) location_id(`location') measure_id(6 9) status(best) source(epi) clear

		preserve
        keep if measure_id==9
		forvalues i = 0/999 {
			quietly generate cfr_`i' = draw_`i'/(12+draw_`i')
		}

		tempfile subhem_cfr
		quietly save `subhem_cfr', replace
		restore

		keep if measure_id==6
		forvalues i = 0/999 {
			quietly rename draw_`i' incidence_`i'
		}

		tempfile subhem_incidence
		quietly save `subhem_incidence', replace

		use `subhem_cfr', clear
		merge 1:1 age_group_id location_id year_id sex_id using `subhem_incidence', keep(3) nogen

        forvalues i = 0/999 {
			generate subhem_`i' = incidence_`i' * (1-cfr_`i')
		}

		drop incidence_* cfr_*

		tempfile subhem
		save `subhem', replace


	// Make ratio
		use `ischemic', clear

		merge 1:1 age_group_id sex_id year_id using `cerhem', keep(3) nogen
		merge 1:1 age_group_id sex_id year_id using `subhem', keep(3) nogen
        forvalues i = 0/999 {
			gen ischemic_ratio_`i' = ischemic_`i'/(ischemic_`i' + cerhem_`i' + subhem_`i')
			gen cerhem_ratio_`i' = cerhem_`i'/(ischemic_`i' + cerhem_`i' + subhem_`i')
			gen subhem_ratio_`i' = subhem_`i'/(ischemic_`i' + cerhem_`i' + subhem_`i')
		}

		egen mean_ischemic = rowmean(ischemic_ratio_*)

		egen mean_cerhem = rowmean(cerhem_ratio_*)

		egen mean_subhem = rowmean(subhem_ratio_*)

		keep age_group_id sex_id year_id mean_ischemic mean_cerhem mean_subhem
		gen location_id = `location'
		tempfile ratios
		save `ratios', replace

save "FILEPATH", replace

log close
