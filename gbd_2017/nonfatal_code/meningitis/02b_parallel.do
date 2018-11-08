// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		
// Last updated:	12/09/2015
// To do: fix temp directory/all file paths, fix settings from step code
// Description:	Parallelization of 02b_etiology_prop

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
	// metric -- measure_id for proportion
	local metric 18

	// get demographics 
	get_demographics, gbd_team(epi) clear
	local years = r(year_id)
	local sexes = r(sex_id)
	local ages = r(age_group_id)

	// define etiology meids
	local meningitis_pneumo_meid 1298
	local meningitis_hib_meid 1328
	local meningitis_meningo_meid 1358
	local meningitis_other_meid 1388

	// write log if running in parallel and log is not already open
	cap log using "FILEPATH", replace
	if !_rc local close 1
	else local close 0


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
// added when get_draws changed in february 2018
run /home/j/temp/central_comp/libraries/current/stata/get_draws.ado
** Pull in the 4 etiologies proportions -- have draws for all etiologies for location/year/sex
	foreach etiology of local etiologies {
		preserve
		get_draws, gbd_id_type(modelable_entity_id) gbd_id(``etiology'_meid') age_group_id(`ages') measure_id(18) location_id(`location') gbd_round_id(5) source(epi) clear
		gen etiology = "`etiology'"
		tempfile data
		save `data', replace
		restore

		append using `data'
	}
	tempfile all_etiologies
	save `all_etiologies', replace
	clear

	foreach year of local years {
		foreach sex of local sexes {
			** Retrieve all DisMod proportion outputs for the etiological fraction, and save them in the clustertmp directory in dta format
			use `all_etiologies', clear
			keep if year_id == `year' & sex_id == `sex'
			keep etiology modelable_entity_id age_group_id draw_*

			** Normalize 1000 draws to add up to 1	
			preserve
			collapse (sum) draw_*, by(age_group_id) fast // adds draws for all four etiologies across identical ages (still 20 rows)
			rename draw_* total_*
			tempfile `location'_`year'_`sex'
			save ``location'_`year'_`sex'', replace
			restore

			merge m:1 age_group_id using ``location'_`year'_`sex'', keep(3) nogen
			
			// normalize draws
			forvalues i = 0/999 {
				qui replace draw_`i' = draw_`i' / total_`i'
				qui drop total_`i'
				qui rename draw_`i' v_`i'
			}
			sort modelable_entity_id age_group_id

			save "FILEPATH", replace
			clear
		}
	}

// write check here
	file open finished using "FILEPATH", replace write
	file close finished

// close logs
if `close' log close
clear
