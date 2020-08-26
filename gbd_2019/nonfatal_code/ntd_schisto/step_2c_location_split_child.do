// Pull the par adjusted base model and split to species based on grs and coinfection splits
//
*** ======================= BOILERPLATE ======================= ***
	clear all
	set more off
	set maxvar 32000
	local user : env USER
	local code_root "FILEPATH`user'"
    local data_root FILEPATH

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
		local params_dir "`data_root'FILEPATH"
		local draws_dir "`data_root'FILEPATH"
		local interms_dir "`data_root'FILEPATH"
		local logs_dir "`data_root'FILEPATH"
		local location 10
	}

	cap log using "`logs_dir'/FILEPATH`location'.smcl", replace
	if !_rc local close_log 1
	else local close_log 0

	// Source relevant functions
	adopath + FILEPATH
    local gbd_round_id 7
    local decomp_step "step2"

*** ======================= MAIN EXECUTION ======================= ***
    // Set up location data ( used in later split == 1 conditional)
	get_location_metadata, location_set_id(9) clear
    keep if most_detailed == 1 & is_estimate == 1
    keep ihme_loc_id location_id location_name region_name
    tempfile location_data
    save `location_data'
    clear

    // pull in species-specific geographic exclusions
	import delimited "`params_dir'/FILEPATH", clear
	keep if location_id == `location'

    // create var indicating if location needs to be split across species
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
	get_draws, gbd_id_type(modelable_entity_id) gbd_id(ADDRESS) source(epi) measure_id(5) location_id(`location') status(best) sex_id(1 2) gbd_round_id(`gbd_round_id') decomp_step(`decomp_step') clear
	// clean files
	drop model_version_id modelable_entity_id
    keep metric_id measure_id location_id year_id age_group_id sex_id draw_*

    forvalues i = 0/999 {
        replace draw_`i' = 0 if age_group_id < 5 | age_group_id == 164
    }

    if `haem' == 1 {
        ** export values for haematobium
        gen modelable_entity_id = ADDRESS
        export delimited "`draws_dir'/ADDRESS/`location'.csv", replace
        forvalues i = 0/999 {
            replace draw_`i' = 0
        }
        ** save 0s as mansoni
        replace modelable_entity_id = ADDRESS
        export delimited "`draws_dir'/ADDRESS/`location'.csv", replace
    }
    if `mans' == 1 {
        ** save values as mansoni
        gen modelable_entity_id = ADDRESS
        export delimited "`draws_dir'/ADDRESS/`location'.csv", replace
        forvalues i = 0/999 {
            replace draw_`i' = 0
        }
        ** save 0s as haematobium
        replace modelable_entity_id = ADDRESS
        export delimited "`draws_dir'/ADDRESS/`location'.csv", replace
    }

    // check if loc needs to be split by haematobium and mansoni
    if `split' == 1 {
        // get region name in there
        merge m:1 location_id using `location_data', nogen keep(3)
        levelsof(region_name), local(region)
        preserve
        // bring in glm species split results
        import delimited "`interms_dir'FILEPATH", clear
        keep if region_name == `region'
        forvalues i = 0/999 {
            qui gen draw_h_`i' = rnormal(prop_h, se_h)
            replace draw_h_`i' = 1 if draw_h_`i' > 1
            replace draw_h_`i' = 0 if draw_h_`i' < 0
            qui gen draw_m_`i' = rnormal(prop_m, se_m)
            replace draw_m_`i' = 1 if draw_m_`i' > 1
            replace draw_m_`i' = 0 if draw_m_`i' < 0
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
        capture mkdir "`draws_dir'FILEPATH"
        export delimited "`draws_dir'FILEPATH`location'.csv", replace
        rename draw_h_* draw_*
        keep metric_id measure_id location_id year_id age_group_id sex_id draw_*
        gen modelable_entity_id = ADDRESS
        export delimited "`draws_dir'/ADDRESS/`location'.csv", replace
        restore, preserve
        drop draw_h* se_* region_name
        export delimited "`draws_dir'/FILEPATH`location'.csv", replace
        rename draw_m_* draw_*
        keep metric_id measure_id location_id year_id age_group_id sex_id draw_*
        gen modelable_entity_id = ADDRESS
        export delimited "`draws_dir'/ADDRESS/`location'.csv", replace			
        restore
    }

*** ======================= CLOSE LOG ======================= ***
	if `close_log' log close
	exit, STATA clear
