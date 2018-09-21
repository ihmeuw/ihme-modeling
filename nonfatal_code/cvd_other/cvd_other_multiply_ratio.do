// Set up environment with necessary settings and locals

// Boilerplate
	clear all
	set maxvar 10000
	set more off
  
	adopath + "FILEPATH"
  
// Pull in parameters from bash command
	local location "`1'"
	
// Start log	
	capture log close
	log using FILEPATH/meps_`location', replace
	
// Locals
	local tmp_dir "FILEPATH"
	local years 1990 1995 2000 2005 2010 2016
	local sexes 1 2
	
// Pull in meps ratio
	use `tmp_dir'/meps_draws/ratio_merge_wide.dta, clear
	tempfile meps
	save `meps', replace
	
// Get draws for overall heart failure due to other
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(9575) source(dismod) measure_ids(5) location_ids(`location') status(best) clear
		
// Merge with MEPS
	merge m:1 sex_id age_group_id using `meps', keep(3) nogen
	forvalues j = 0/999 {
		quietly gen draw_mod_`j' = (draw_`j' / (1 - draw_`j')) * coef_`j' // Here we multiply the prevalence odds of all countries for cvd_other due to HF, by the MEPS-generated ratio of odds(cvd_other) / odds(cvd_other_hf)
		quietly rename draw_`j' draw_old_`j'
		quietly gen draw_`j' = draw_mod_`j' / (1 + draw_mod_`j') // Finally, we convert from the prevalence odds(cvd_other) to prevalence(cvd_other)
		quietly replace draw_`j' = 0 if draw_`j'<0 | draw_`j'==.
	}
	
tempfile master_other
save `master_other', replace

// Save Other CVD (excluding HF deaths)
	use `master_other', clear
	forvalues j = 0/999 {
		quietly replace draw_`j' = draw_`j' - draw_old_`j' // Take all-other prevalence minus other-due-to-HF prevalence
		quietly replace draw_`j' = 0 if draw_`j'<0 | draw_`j'==.
	}

	drop draw_mod_* draw_old_*
	
	foreach sex of local sexes {
		foreach year of local years {
			preserve
			keep if sex_id==`sex' & year_id==`year'
			outsheet age_group_id draw_* using "FILEPATH/5_`location'_`year'_`sex'.csv", comma replace
			restore
		}
	}

log close
	