// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		
// Last updated:	12/17/2015
// To do: Check up on directories/file paths
// Description:	Parallelization of 02c_etiology_split

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set mem 2g
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
	// functional
	local functional "meningitis"
	// define etiologies
	local etiologies "meningitis_pneumo meningitis_hib meningitis_meningo meningitis_other"
	// measure_id for incidence
	local metric 6

	// get demographics 
	get_demographics, gbd_team(epi) clear
	local years = r(year_id)
	local sexes = r(sex_id)
	local ages = r(age_group_id)

	// set etiology meids
	local meid 1296 // meningitis meid
	local meningitis_pneumo_meid 1298
	local meningitis_hib_meid 1328
	local meningitis_meningo_meid 1358
	local meningitis_other_meid 1388

	// set input file paths
	local pull_dir_02a "FILEPATH"
	local pull_dir_02b "FILEPATH"

	// write log if running in parallel and log is not already open
	cap log using "FILEPATH", replace
	if !_rc local close 1
	else local close 0

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
	// added when get_draws changed in february 2018
	run "FILEPATH"
	* pull in meningitis from DisMod, 
	get_draws, gbd_id_type(modelable_entity_id) gbd_id(`meid') age_group_id(`ages') measure_id(`metric') location_id(`location') source(epi) gbd_round_id(5) clear
	drop model_version_id measure_id

	foreach year of local years {
		foreach sex of local sexes {
			foreach etiology of local etiologies {
				preserve
				keep if year_id == `year' & sex_id == `sex'
				gen etiology = "`etiology'"
				replace modelable_entity_id = ``etiology'_meid'
				// pull 02b outputs
				local split "FILEPATH"
				// pull 02a outputs
				local survive "FILEPATH"

				merge 1:1 age_group_id etiology using `split', keep(3) nogen

				forvalues c = 0/999 {
					qui replace draw_`c' = draw_`c' * v_`c' // multiplying proportion * incidence dm_draws --> proportional incidence of specific etiology
					qui drop v_`c'
				}
				
				merge 1:1 age_group_id using `survive', keep(3) nogen

				foreach i of numlist 0/999 {
					qui replace draw_`i' = draw_`i' * v_`i' // proportional incidence of specific etiology * survival proportion = likelihood of surviving an etiology (no conditionals, this is over the entire population)
					qui drop v_`i'
				}

				gen measure_id = 6
				order modelable_entity_id measure_id etiology location_id year_id age_group_id draw_*

				save "`tmp_dir'/03_outputs/01_draws/`etiology'_`location'_`year'_`sex'.dta", replace
				// outputs draws of a rate of survival: survivors of a specific etiology / total population
				restore
			}
		}
	}

// write check here
	file open finished using "FILEPATH", replace write
	file close finished

// close logs
	if `close' log close
	clear
		
