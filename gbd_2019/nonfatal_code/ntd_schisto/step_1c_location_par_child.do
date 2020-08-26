// Use proportion at risk draws from raster to adjust model for population at risk
//
*** ======================= BOILERPLATE ======================= ***
	clear all
	set more off
	set maxvar 32000
	local user : env USER
	local code_root FILEPATH`user'"
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
		local location 491
	}

	cap log using "`logs_dir'/FILEPATH`location'.smcl", replace
	if !_rc local close_log 1
	else local close_log 0

	// Source relevant functions
	adopath + FILEPATH
    local gbd_round_id 7
    local decomp_step "step2"
*** ======================= MAIN EXECUTION ======================= ***

    // Pull Unadjusted prevalence draws
    di "pulling schisto unadjusted prevalence draws"
	// prevalence, all years, this location
	get_draws, gbd_id_type(modelable_entity_id) gbd_id(ADDRESS) source(epi) measure_id(5) location_id(`location') version_id(ADDRESS) sex_id(1 2) gbd_round_id(`gbd_round_id') decomp_step(`decomp_step') clear
	replace measure_id = 5
	// clean files
	drop model_version_id modelable_entity_id
	tempfile draws
	save `draws', replace

    if inlist(location_id, 491,494,496,497,503,504,506,507,514,516,520,521) {
        import delimited  "`params_dir'FILEPATH", clear
        keep if location_id == `location'
        merge 1:m location_id year_id using `draws', nogen keep(3)
	}
    else {
        use "`params_dir'FILEPATH", clear
        keep if location_id == `location'
        merge 1:m location_id using `draws', nogen keep(3)
    }

    forvalues i = 0/999 {
        replace draw_`i' = draw_`i' * prop_`i'
    }

    ** Turkey eliminated schisto in 2003
    if `location' == 155 {
        forvalues i = 0/999 {
            replace draw_`i' = 0 if year_id > 2002
        }
    }
    ** Iran eliminated schisto in 2002
    if `location' == 44882 {
        forvalues i = 0/999 {
            replace draw_`i' = 0 if year_id > 2011
        }
    }
	
    ** save file
	gen modelable_entity_id=ADDRESS
    keep modelable_entity_id metric_id measure_id location_id year_id age_group_id sex_id draw_*
    export delimited "`draws_dir'/FILEPATH/`location'.csv", replace


*** ======================= CLOSE LOG ======================= ***
	if `close_log' log close
	exit, STATA clear
