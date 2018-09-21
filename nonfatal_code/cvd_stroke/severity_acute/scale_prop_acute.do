// Pull in Rankin scores; scale

// Prep Stata
	clear all
	set more off
	set maxvar 32767 
	
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}
// Add adopaths
	adopath + "FILEPATH"
	
// Locals for file paths
	local tmp_dir "FILEPATH"
	local logdir "FILEPATH"
	
// Get inputs from bash commmand
	local location "`1'"
		
// write log if running in parallel and log is not already open
	cap log close
	log using "`logdir'/log_rankinacute_`location'.smcl", replace

// Get locations and demographic information; make macros
	local location 1
	local year_ids 2016
	local sex_ids 1 2 

	
// Pull in the draws from the 5 proportion models and scale to 1 (no asymptomatic for acute stroke)
	//Mild
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(3956) source(dismod) measure_ids(18) location_ids(`location') year_ids(2016) status(best) clear
	drop measure_id model_version_id modelable_entity_id
	forvalues j = 0/999 {
		rename draw_`j' mild_`j'
	}
	reshape long mild_, i(age_group_id sex_id year_id)
	tempfile mild
	save `mild', replace

	//Moderate
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(3957) source(dismod) measure_ids(18) location_ids(`location') year_ids(2016) status(best) clear
	drop measure_id model_version_id modelable_entity_id
	forvalues j = 0/999 {
		rename draw_`j' mod_`j'
	}
	reshape long mod_, i(age_group_id sex_id year_id)
	tempfile mod
	save `mod', replace
	
	//Moderate + cognitive
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(3958) source(dismod) measure_ids(18) location_ids(`location') year_ids(2016) status(best) clear
	drop measure_id model_version_id modelable_entity_id
	forvalues j = 0/999 {
		rename draw_`j' mod_cog_`j'
	}
	reshape long mod_cog_, i(age_group_id sex_id year_id)
	tempfile mod_cog
	save `mod_cog', replace
	
	//Severe
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(3959) source(dismod) measure_ids(18) location_ids(`location') year_ids(2016) status(best) clear
	drop measure_id model_version_id modelable_entity_id
	forvalues j = 0/999 {
		rename draw_`j' sev_`j'
	}
	reshape long sev_, i(age_group_id sex_id year_id)
	tempfile sev
	save `sev', replace
	
	//Severe + cog
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(3960) source(dismod) measure_ids(18) location_ids(`location') year_ids(2016) status(best) clear
	drop measure_id model_version_id modelable_entity_id
	forvalues j = 0/999 {
		rename draw_`j' sev_cog_`j'
	}
	reshape long sev_cog_, i(age_group_id sex_id year_id)
	tempfile sev_cog
	save `sev_cog', replace
	
	
	//Merge and rescale
	clear
	tempfile results
	save `results', emptyok
	use `mild', clear
	merge 1:1 age_group_id sex_id year_id _j using `mod', keep(3) nogen
	save `results', replace
	merge 1:1 age_group_id sex_id year_id _j using `mod_cog', keep(3) nogen
	save `results', replace
	merge 1:1 age_group_id sex_id year_id _j using `sev', keep(3) nogen
	save `results', replace
	merge 1:1 age_group_id sex_id year_id _j using `sev_cog', keep(3) nogen
	save `results', replace
	
	use `results', clear 
	egen scale_factor = rowtotal(mild_ mod_ mod_cog_ sev_ sev_cog_) 
	tempfile results_scaled
	save `results_scaled', replace

	use `results', clear
	merge 1:1 age_group_id year_id sex_id _j using `results_scaled', keepusing(scale_factor) keep(3) nogen
	tempfile results_merged
	save `results_merged', replace

	clear
	tempfile proportions
	save `proportions', emptyok
	use `results_merged', clear
	local subtype "mild mod mod_cog sev sev_cog"
	foreach type of local subtype {
		replace `type'_  = `type'_ / scale_factor // Adjusting for fractions above or below 1, making sure all adds up to 1
		preserve
		keep `type'_ age_group_id sex_id year_id _j location_id
		reshape wide `type', i(age_group_id sex_id year_id) j(_j)
		fastpctile `type'*, pct(2.5 97.5) names(lower_`type' upper_`type')
		fastrowmean `type'*, mean_var_name(mean_`type')
		drop `type'_*
		append using `proportions'
		save `proportions', replace
		restore
	}

	use `proportions', clear
	
	collapse (mean) mean_* lower_* upper_*, by(sex_id age_group_id) 
	save `tmp_dir'/ages_`location'.dta, replace
	save FILEPATH/rankin_acute_ages.dta, replace
		

log close
