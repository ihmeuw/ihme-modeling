// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		
// Last updated:	2:51 PM 6/26/2014
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
	adopath + "FILEPATH"
	// define which sequelae to estimate (Y=1, N=0)
	local mort0 = 1
	local mort1 = 1
	// grouping
	local grouping "long_mild long_modsev" // took out _vision, hopefully uploading it earlier
	// functional
	local functional "encephalitis"

	// get locals from demographics
	get_demographics, gbd_team(epi) clear
	local years = r(year_id)
	local sexes = r(sex_id)

	// directory for pulling files from previous step
	local pull_dir_04b "FILEPATH"
	local pull_dir_04a "FILEPATH"
	// split
	local pull_dir_05a "FILEPATH"
	
	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/02_temp/02_logs/`step_num'_`location'.smcl", replace
	if !_rc local close 1
	else local close 0
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
** Pull in the etiology-outcome file

	foreach year of local years {
		foreach sex of local sexes {
			foreach group of local grouping {
				if "`group'" == "long_mild" local file "FILEPATH"
				// if "`group'" == "_vision" local file "`pull_dir_04b'/`functional'_`group'_`location'_`year'_`sex'.dta"
				if "`group'" == "long_modsev" local file "FILEPATH"
				local split "FILEPATH"


				local conduct = 0
				if "`group'" == "long_mild" local conduct = `mort0'
				// if "`group'" == "_vision" local conduct = `mort0'
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

					//load in correct data type either ODE or 4b step
					foreach sequela of local sequelae {
						if "`group'" == "long_mild" use "`file'", clear
						else if "`group'" == "long_modsev" import delimited "`file'", clear

						merge m:1 measure_id using `split_`sequela'', keep(3) nogen

						forvalues c = 0/999 {
							qui replace draw_`c' = draw_`c' * v_`c'
							qui drop v_`c'
						}

						capture drop code state
						save "FILEPATH", replace
					}
				}
			}
		}
	}

	// write check here
	file open finished using "FILEPATH", replace write
	file close finished
	
	if `close' log close
	clear
