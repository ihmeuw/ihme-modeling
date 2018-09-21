// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		
// Last updated:	
// Description:	Parallelization of 05b_sequela_split_woseiz
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

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
	local date 			`1'
	local step_num 		`2'
	local step_name		`3'
	local location 		`4'
	local code_dir 		`5'
	local in_dir 		`6'
	local out_dir 		`7'
	local tmp_dir 		`8'
	local root_tmp_dir 	`9'
	local root_j_dir 	`10'

// define other locals
	// directory for standard code files
	adopath + "SHARED FUNCTIONS"
	// define which sequelae to estimate (Y=1, N=0)
	local mort0 = 1
	local mort1 = 1
	// grouping
	local grouping "long_mild long_modsev" 
	// functional
	local functional "encephalitis"
	// get locals from demographics
	get_demographics, gbd_team(epi) clear
	local years = r(year_ids)
	local sexes = r(sex_ids)

	// directory for pulling files from previous step
	local pull_dir_04b "`root_tmp_dir'/03_steps/`date'/04b_outcome_prev_womort/03_outputs/01_draws"
	local pull_dir_04d "`root_tmp_dir'/03_steps/`date'/04d_outcome_prev_wmort_woseiz/03_outputs/01_draws"
	// split
	local pull_dir_05a "`root_tmp_dir'/03_steps/`date'/05a_sequela_prop/03_outputs/01_draws"
	
	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/02_temp/02_logs/`step_num'_`location'.smcl", replace
	if !_rc local close 1
	else local close 0
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
** Pull in the etiology-outcome file

	// test run
	//local sexes 1
	// define years
	//local years 2000 2005

	foreach year of local years {
		foreach sex of local sexes {
			foreach group of local grouping {
				if "`group'" == "long_mild" local file "`pull_dir_04b'/`functional'_`group'_`location'_`year'_`sex'.dta"
				if "`group'" == "long_modsev" local file "`pull_dir_04d'/`functional'_`group'_`location'_`year'_`sex'.dta"
				local split "`out_dir'/02_temp/03_data/prop_`sequela'.dta"


				local conduct = 0
				if "`group'" == "long_mild" local conduct = `mort0'
				if "`group'" == "long_modsev" local conduct = `mort1'
				if `conduct' == 1 {
							
					use "`pull_dir_05a'/`functional'_`group'.dta", clear
					drop if state == "asymptomatic"
					levelsof state, local(sequelae) clean

					preserve
					foreach sequela of local sequelae {
						keep if state == "`sequela'"
						tempfile split_`sequela'
						save `split_`sequela'', replace
						restore, preserve
					}
					capture restore, not
					clear

					foreach sequela of local sequelae {
						use "`file'", clear
						merge m:1 measure_id using `split_`sequela'', keep(3) nogen

						forvalues c = 0/999 {
							qui replace draw_`c' = draw_`c' * v_`c'
							qui drop v_`c'
						}

						capture drop code state
						save "`tmp_dir'/03_outputs/01_draws/`functional'_`sequela'_`location'_`year'_`sex'.dta", replace
					}
				}
			}
		}
	}

	// write check here
	file open finished using "`tmp_dir'/02_temp/01_code/checks/finished_loc`location'.txt", replace write
	file close finished
	
	if `close' log close
	clear
