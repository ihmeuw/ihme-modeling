// Pull the par adjusted base model and split to species based on grs and coinfection splits
//
*** ======================= BOILERPLATE ======================= ***
	clear all
	set more off
	set maxvar 32000
	local user : env USER
	local code_root "FILEPATH"
    local data_root "FILEPATH"

	local exec_from_args : env EXEC_FROM_ARGS
	capture confirm variable exec_from_args, exact
	if "`exec_from_args'" == "True" {
		local params_dir 		`2'
		local draws_dir 		`4'
		local interms_dir		`6'
		local logs_dir 			`8'
		local location			`10'
	}
	else {
		local params_dir "FILEPATH"
		local draws_dir "FILEPATH"
		local interms_dir "FILEPATH"
		local logs_dir "FILEPATH"
		local location 10
	}

	cap log using "FILEPATH/step_2c_`location'.smcl", replace
	if !_rc local close_log 1
	else local close_log 0

	// Source relevant functions
	adopath + FILEPATH
    local release_id ADDRESS
*** ======================= MAIN EXECUTION ======================= ***
    // Set up location data ( used in later split == 1 conditional)
	get_location_metadata, location_set_id(9) clear
    keep if most_detailed == 1 & is_estimate == 1
    keep ihme_loc_id location_id location_name region_name
    tempfile location_data
    save `location_data'
    clear

    // pull in species-specific geographic exclusions
	import delimited "FILEPATH/species_specific_exclusions.csv", clear
	keep if location_id == `location'

	// does this location need to be split across species?
	local split = 0
	if haematobium == "" & mansoni == "" {
		local split = 1
	}
	
	local haem = 0
	local mans = 0
	if `split' == 0 {
		if haematobium == "" {
			local haem = 1
		}
		else if haem != "" {
			local mans = 1
		}
	}

	// get draws from schisto unadjusted prevalence model
	di "pulling schisto adjusted prevalence draws"
	// prevalence, all years, this location
	get_draws, gbd_id_type(model_id) gbd_id(ADDRESS) source(ADDRESS) measure_id(5) location_id(`location') status(best) sex_id(1 2) release_id(`release_id') clear
	// clean files
	drop version_id model_id
    keep metric_id measure_id location_id year_id age_group_id sex_id draw_*

    forvalues i = 0/999 {
        replace draw_`i' = 0 if age_group_id < ID | age_group_id == ID
    }

    if `haem' == 1 {
        ** export values for haematobium
        gen model_id= ADDRESS
        export delimited "FILEPATH/`location'.csv", replace
        forvalues i = 0/999 {
            replace draw_`i' = 0
        }
        ** save 0s as mansoni
        replace model_id= ADDRESS
        export delimited "FILEPATH/`location'.csv", replace
    }
    if `mans' == 1 {
        ** save values as mansoni
        gen model_id= ADDRESS
        export delimited "FILEPATH/`location'.csv", replace
        forvalues i = 0/999 {
            replace draw_`i' = 0
        }
        ** save 0s as haematobium
        replace model_id= ADDRESS
        export delimited "FILEPATH/`location'.csv", replace
    }

    // check to see if this country needs to be split by haematobium and mansoni
    if `split' == 1 {
        // get region name in there
        merge m:1 location_id using `location_data', nogen keep(3)
        levelsof(region_name), local(region)
        preserve
        // bring in glm species split results
        import delimited "FILEPATH.csv", clear
        keep if region_name == `region'
        forvalues i = 0/999 {
            qui gen draw_h_`i' = rnormal(prop_h, se_h)
            replace draw_h_`i' = 1 if draw_h_`i' > 1
            replace draw_h_`i' = 0 if draw_h_`i' < 0
            replace draw_h_`i' = rnormal(.1, se_h) if `location' == 190
            qui gen draw_m_`i' = rnormal(prop_m, se_m)
            replace draw_m_`i' = 1 if draw_m_`i' > 1
            replace draw_m_`i' = 0 if draw_m_`i' < 0
            replace draw_m_`i' = rnormal(.9, se_m) if `location' == 190
            gen total_`i' = draw_h_`i' + draw_m_`i'
            ** adjust to make sure the sum of each pair of draws is at least 1
            replace draw_h_`i' = draw_h_`i' * (1/total_`i') if total_`i' < 1
            replace draw_m_`i' = draw_m_`i' * (1/total_`i') if total_`i' < 1
        }
        gen location_id = `location'
        tempfile prop_draws
        save `prop_draws', replace
        // pull in prevalence draws
        restore
        // multiply draws to get species-specific prevalence
        merge m:1 location_id using `prop_draws', nogen keep(3)
        forvalues i = 0/999 {
            qui replace draw_h_`i' = draw_`i' * draw_h_`i'
            qui replace draw_m_`i' = draw_`i' * draw_m_`i'
            qui drop draw_`i'
        }
        // save species-specific draws by location
        preserve
        drop draw_m* se_* region_name
        capture mkdir "FILEPATH"
        export delimited "FILEPATH/`location'.csv", replace
        rename draw_h_* draw_*
        keep metric_id measure_id location_id year_id age_group_id sex_id draw_*
        gen model_id= ADDRESS
        export delimited "FILEPATH/`location'.csv", replace
        restore, preserve
        drop draw_h* se_* region_name
        export delimited "FILEPATH/`location'.csv", replace
        rename draw_m_* draw_*
        keep metric_id measure_id location_id year_id age_group_id sex_id draw_*
        gen model_id= ADDRESS
        export delimited "FILEPATH/`location'.csv", replace			
        restore
    }

*** ======================= CLOSE LOG ======================= ***
	if `close_log' log close
	exit, STATA clear
