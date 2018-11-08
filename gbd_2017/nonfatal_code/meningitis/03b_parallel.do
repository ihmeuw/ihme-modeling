// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		
// Last updated:	12/31/2015
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
	adopath + "FILEPATH"
	// functional
	local functional "meningitis"
	// define etiologies
	local etiologies "meningitis_pneumo meningitis_hib meningitis_meningo meningitis_other"
	// define groupings
	local grouping "_hearing long_mild _vision long_modsev epilepsy"

	// get demographics
	get_demographics, gbd_team(epi) clear
	local years = r(year_id)
	local sexes = r(sex_id)

	// set input file paths
	local pull_dir_02c "FILEPATH"
	local pull_dir_03a "FILEPATH"

	// write log if running in parallel and log is not already open
	cap log using "FILEPATH", replace
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
					local file "FILEPATH"
					// pull major proportion draws (03a)
					local split "FILEPATH"

					** Pull in the meningitis-etiology DisMod file
					use `file', clear
					merge m:1 measure_id modelable_entity_id location_id year_id using `split', keep(3) nogen

					forvalues c = 0/999 {
						qui replace draw_`c' = draw_`c' * v_`c' // multiplying incidence of etiology by major proportional draws = incidence of sequela
						qui drop v_`c'
					}
					order measure_id modelable_entity_id etiology grouping location_id year_id age_group_id draw_*
					save "FILEPATH", replace
					
				}
			}
		}
	}
	
// write check here
	file open finished using "FILEPATH", replace write
	file close finished

// close logs
if `close' log close
clear
