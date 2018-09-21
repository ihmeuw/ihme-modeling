// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		
// Last updated:	
// Description:	Parallelization of 07_etiology_prev

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
	// healthstate
	local healthstate "inf_sev"
	// directory for standard code files
	adopath + "SHARED FUNCTIONS"
	// etiologies
	local etiologies "meningitis_pneumo meningitis_hib meningitis_meningo meningitis_other"
	// metric
	local metric 5
	// directory for pulling files from previous step
	local pull_dir_02b "`root_tmp_dir'/03_steps/`date'/02b_etiology_prop/03_outputs/01_draws"

	// get locals from demographics
	get_demographics, gbd_team(epi) clear
	local years = r(year_ids)
	local sexes = r(sex_ids)
	local age_group_ids = r(age_group_ids)

	//local years 2000 2005
	//local sexes 1 

	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/02_temp/02_logs/`step_num'_`location'.smcl", replace
	if !_rc local close 1
	else local close 0
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE

foreach etiology of local etiologies {
	cap mkdir "`tmp_dir'/03_outputs/01_draws/`etiology'"

	** pull draws from best meningitis (1296) DisMod model
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(1296) measure_ids(5) location_ids(`location') status(best) source(epi) age_group_ids(`age_group_ids') clear
	gen etiology = "`etiology'"

	foreach year of local years {
		foreach sex of local sexes {
			preserve
			keep if year_id == `year'
			keep if sex_id == `sex'
			** call 02b output
			local split "`pull_dir_02b'/fractions_`location'_`year'_`sex'_general.dta"
			merge m:1 age_group_id etiology using `split', keep(3) nogen

			forvalues c = 0/999 {
				qui replace draw_`c' = draw_`c' * v_`c'
				qui drop v_`c'
			}

			order measure_id etiology location_id year_id age_group_id sex_id draw_*
			save "`tmp_dir'/03_outputs/01_draws/`etiology'/`etiology'_`healthstate'_`location'_`year'_`sex'.dta", replace
			restore
		}
	}



}

	// write check here
	file open finished using "`tmp_dir'/02_temp/01_code/checks/finished_loc`location'.txt", replace write
	file close finished	

	if `close' log close
	clear
