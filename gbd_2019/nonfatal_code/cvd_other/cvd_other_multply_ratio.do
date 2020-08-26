// Set up environment with necessary settings and locals

// Boilerplate
	clear all
	set maxvar 10000
	set more off

	adopath + FILEPATH

// Pull in parameters from bash command
	local location "`1'"

// Start log
	capture log close
	log using FILEPATH, replace

// Locals
	local tmp_dir FILEPATH
	local years 1990 1995 2000 2005 2010 2015 2017 2019
	local sexes 1 2

// Pull in meps ratio
	use `tmp_dir'/FILEPATH/ratio_merge_wide.dta, clear
	tempfile meps
	save `meps', replace

// Get draws for overall heart failure due to other
	get_draws, gbd_id_type(modelable_entity_id) gbd_id(9575) source(epi) measure_id(5) location_id(`location') status(best) gbd_round_id(6) decomp_step("step4") clear
	//updated get_draws command to work with updated syntax 26Jul2018

// Merge with MEPS
	merge m:1 sex_id age_group_id using `meps', keep(3) nogen
	forvalues j = 0/999 {
		quietly gen draw_mod_`j' = (draw_`j' / (1 - draw_`j')) * coef_`j' // Here we multiply the odds ratios of all countries for cvd_other due to HF, by the MEPS-generated ratio of odds(cvd_other) / odds(cvd_other_hf)
		quietly rename draw_`j' draw_old_`j'
		quietly gen draw_`j' = draw_mod_`j' / (1 + draw_mod_`j') // Finally, we convert from the odds ratio(cvd_other) to prevalence(cvd_other)
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

	outsheet age_group_id sex_id year_id draw_* using  "FILEPATH/5_`location'.csv", comma replace
log close
