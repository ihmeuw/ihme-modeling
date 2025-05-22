// Use proportion at risk draws from raster to adjust model for population at risk
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
	}

	cap log using "FILEPATH/step_1_`location'.smcl", replace
	if !_rc local close_log 1
	else local close_log 0

	// Source relevant functions
	adopath + FILEPATH
    local release_id ADDRESS
*** ======================= MAIN EXECUTION ======================= ***

    // Pull Unadjusted prevalence draws
    di "pulling schisto unadjusted prevalence draws"
	// prevalence, all years, this location
	get_draws, gbd_id_type(model_id) gbd_id(ADDRESS) source(ADDRESS) measure_id(5) location_id(`location') version_id(ADDRESS) sex_id(1 2) release_id(`release_id') clear
	replace measure_id = 5
	// clean files
	drop version_id model_id
	tempfile draws
	save `draws', replace

    if inlist(location_id, LIST) {
        import delimited  "FILEPATH.csv", clear
        keep if location_id == `location'
        merge 1:m location_id year_id using `draws', nogen keep(3)
	}
    else {
        use "FILEPATH.dta", clear
        keep if location_id == `location'
        merge 1:m location_id using `draws', nogen keep(3)
    }

    forvalues i = 0/999 {
        replace draw_`i' = draw_`i' * prop_`i'
    }

    ** Turkey eliminated schisto in 2003
    if `location' == ID {
        forvalues i = 0/999 {
            replace draw_`i' = 0 if year_id > 2002
        }
    }
    ** Iran eliminated schisto in 2002
    if `location' == ID {
        forvalues i = 0/999 {
            replace draw_`i' = 0 if year_id > 2011
        }
    }
	
    ** save file
	gen model_id=ADDRESS
    keep model_idmetric_id measure_id location_id year_id age_group_id sex_id draw_*
    export delimited "FILEPATH/`location'.csv", replace


*** ======================= CLOSE LOG ======================= ***
	if `close_log' log close
	exit, STATA clear
