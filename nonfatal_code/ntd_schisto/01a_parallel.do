	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "J:"
	}

// define locals from qsub command
	local location 		`1'
	
	local outDir FILEPATH

	// write log if running in parallel and log is not already open
	capture log close
	log using "FILEPATH/01a_`location'.smcl", replace


	// directory for standard code file
	adopath + FILEPATH
	adopath + FILEPATH


	local years 1990 1995 2000 2005 2010 2016
	local sexes 1 2

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
** this code first...
	* pulls in unadjusted prevalence estimates from dismod
	* applies general geographic restrictions to all prevalence results
	* applies population at risk to get adjusted prevalence

	// pull in species-specific geographic exclusions
	import delimited "FILEPATH/species_specific_exclusions.csv", clear
	keep if location_id == `location'
	local keep = 1
	if haematobium != "" & mansoni != "" & japonicum != "" & other != "" {
		local keep = 0
	}


	di "pulling schisto unadjusted prevalence draws"
	// prevalence, all years, this location
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(10537) source(epi) measure_ids(5) location_ids(`location')  model_version_id(173411) sex_ids(1 2) clear
	
	// clean files
	drop model_version_id modelable_entity_id measure_id
	generate measure_id = 5
	tempfile draws
	save `draws', replace

	// if this is a universal geographic exclusion, replace draws with 0
	if `keep' == 0 {
		use `draws', clear
		forvalues i = 0/999 {
			qui replace draw_`i' = 0
		}
		// save file
		keep measure_id location_id year_id age_group_id sex_id draw_*
		export delimited "FILEPATH/`location'.csv", replace
	}

	if `keep' == 1 {
		** pull in population at risk estimates
		if inlist(location_id, 491,494,496,497,503,504,506,507,514,516,520,521) {
			use  "FILEPATH/prop_draws_chn.dta", clear
			keep if location_id == `location'
			merge 1:m location_id year_id using `draws', nogen keep(3)
			}
		else {
			use  "FILEPATH/prop_draws.dta", clear
			keep if location_id == `location'
			merge 1:m location_id using `draws', nogen keep(3)
			}
			
		forvalues i = 0/999 {
			replace draw_`i' = draw_`i' * prop_`i'
			}
			
		if `location' == 206 {
			egen drawmean = rowmean(draw_*)
			sum  drawmean if year_id<2016
			local mean = `r(mean)'
			sum  drawmean if year_id==2016
			local mean2016 = `r(mean)'
			forvalues i = 0 / 999 {
				replace draw_`i' = draw_`i' * `mean' / `mean2016' if year_id==2016
				}
			}
			
		** save file
		keep measure_id location_id year_id age_group_id sex_id draw_*
		export delimited "FILEPATH/`location'.csv", replace
	}


** write check here
	file open progress using FILEPATH/`location'.txt, text write replace
	file write progress "complete"
	file close progress

** close logs
	log close

	
