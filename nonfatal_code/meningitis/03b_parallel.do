// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		
// Last updated:	
// To do: Check file paths and directories
// Description:	Parallelization of 03b_outcome_split

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
	adopath + "SHARED FUNCTIONS"
	// functional
	local functional "meningitis"
	// define etiologies
	local etiologies "meningitis_pneumo meningitis_hib meningitis_meningo meningitis_other"
	// define groupings
	local grouping "_hearing long_mild _vision long_modsev _epilepsy"

	// get demographics
	get_demographics, gbd_team(epi) clear
	local years = r(year_ids)
	local sexes = r(sex_ids)

	// test run
	/*
	local years 2000 2005
	local sexes 1
	*/
	// set input file paths
	local pull_dir_02c "`root_tmp_dir'/03_steps/`date'/02c_etiology_split/03_outputs/01_draws"
	local pull_dir_03a "`root_tmp_dir'/03_steps/`date'/03a_outcome_prop/03_outputs/01_draws"

	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/02_temp/02_logs/`step_num'_`location'.smcl", replace
	if !_rc local close 1
	else local close 0
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
	foreach year of local years {
		foreach sex of local sexes {
			foreach etiology of local etiologies {
				foreach group of local grouping {
					// pull survival rate from etiology split (02c)
					local file "`pull_dir_02c'/`etiology'_`location'_`year'_`sex'.dta"
					// pull major proportion draws (03a)
					local split "`pull_dir_03a'/risk_`etiology'_`group'.dta"

					** Pull in the meningitis-etiology DisMod file
					use `file', clear
					merge m:1 measure_id modelable_entity_id location_id year_id using `split', keep(3) nogen

					forvalues c = 0/999 {
						qui replace draw_`c' = draw_`c' * v_`c' // multiplying incidence of etiology by major proportional draws = incidence of sequela
						qui drop v_`c'
					}
					order measure_id modelable_entity_id etiology grouping location_id year_id age_group_id draw_*
					save "`tmp_dir'/03_outputs/01_draws/`etiology'_`group'_`location'_`year'_`sex'.dta", replace
					
				}
			}
		}
	}
	
// write check here
	file open finished using "`tmp_dir'/02_temp/01_code/checks/finished_loc`location'.txt", replace write
	file close finished

// close logs
if `close' log close
clear
