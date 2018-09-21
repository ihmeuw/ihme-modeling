// Prep Stata
	clear all
	set more off
	
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}

// Update adopath
	adopath + FILEPATH

// Set directory paths
	local path "FILEPATH"
	local out_path "FILEPATH"

// PULL IN LOCATION_ID FROM BASH COMMAND
	local location "`1'"
		
// Set locals
	local meid_pvd 2532
	local meid_claudication 1861
	local save_type "symp asymp"
	local years 1990 1995 2000 2005 2010 2016
	local sexes 1 2

// Start log  
	capture log close
	log using `path'/log_`location', replace
	
//Import and Compile Prevalence of PVD from DisMod estimates
	local meid_pvd 2532
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(`meid_pvd') location_ids(`location') measure_ids(5 6) status(best) source(dismod) clear
	preserve
		keep if measure_id==6
		foreach year of local years {
			foreach sex of local sexes {
				outsheet age_group_id draw_* if year_id==`year' & sex_id==`sex' using "`out_path'/asymp/6_`location'_`year'_`sex'.csv", comma replace
			}
		}
	restore
	
	keep if measure_id==5
	forvalues i = 0/999 {
		quietly rename draw_`i' prev_`i'
	}
	tempfile prevalence
	save `prevalence', replace

//Import and Compile Claudication Proportion (percent of people with PVD who show symptoms of claudication) from DisMod estimates
	local meid_claudication 1861
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(`meid_claudication') location_ids(`location') measure_ids(18) status(best) source(epi) clear
	forvalues i = 0/999 {
		quietly rename draw_`i' claud_`i'
	}
	tempfile claudication
	save `claudication', replace


// Merge and Transform Data

// PVD Prevalence is the Effective Sample Size (uncertainty measure), and Claudication_prevalence is # of people with Claudication
	use `prevalence', clear
	merge 1:1 location_id year_id sex_id age_group_id using `claudication', keep(3) nogen

	forvalues i = 0/999 {
		gen symp_`i' = prev_`i' * claud_`i'
		gen asymp_`i' = prev_`i' * (1-claud_`i')
	}
	tempfile master
	save `master'
	
// Saving check file for summary statistics
	local type "prev claud symp asymp"
	foreach kind of local type {
		egen `kind'_mean = rowmean(`kind'_*)
		egen `kind'_lower = rowpctile(`kind'_*), p(2.5)
		egen `kind'_upper = rowpctile(`kind'_*), p(97.5)
		egen `kind'_sd = rowsd(`kind'_*)
	}
	keep *_mean *_upper *_lower *_sd age_group_id sex_id location_id year_id
	save "`out_path'/diagnostics/check_`location'.dta", replace

// Save output file for upload with save results	
	local save_type "symp asymp"
	local years 1990 1995 2000 2005 2010 2016
	local sexes 1 2
	foreach kind of local save_type {
		use `master', clear
		keep age_group_id `kind'_* year_id sex_id
		forvalues i = 0/999 {
			quietly rename `skind'_`i' draw_`i'
			replace draw_`i' = 0 if age_group_id < 13 //set to 0 for those under 40
		}
		
		foreach year of local years {
			foreach sex of local sexes {
				preserve
				keep if year_id==`year' & sex_id==`sex'
				outsheet age_group_id draw_* using "`out_path'/`kind'/5_`location'_`year'_`sex'.csv", comma replace
				restore
			}
		}
	}

log close
	