// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author:		USERNAME
// Description:	multiply short-term incidences by durations to get prevalence of short-term injury outcomes by ecode-ncode-platform, then multiply by DW to get YLDs

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// PREP STATA (DON'T EDIT)

	// prep stata
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "DIRECTORY"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "DIRECTORY"
	}

// If no check global passed from master, assume not a test run
	if "`1'" == "" {
		local 1 FILEPATH
		local 2 FILEPATH
		local 3 DATE
		local 4 "05a"
		local 5 scaled_short_term_en_prev_yld_by_platform

		local 8 "FILEPATH"
	}
	// base directory on FILEPATH 
	local root_j_dir `1'
	// base directory on FILEPATH
	local root_tmp_dir `2'
	// timestamp of current run (i.e. 2014_01_17)
	local date `3'
	// step number of this step (i.e. 01a)
	local step_num `4'
	// name of current step (i.e. first_step_name)
	local step_name `5'
	// step numbers of immediately anterior parent step (i.e. for step 2: 01a 01b 01c)
	local hold_steps `6'
	// step numbers for final steps that you are running in the current run (i.e. 11a 11b 11c)
	local last_steps `7'
    // directory where the code lives
    local code_dir `8'
	// directory for external inputs
	local in_dir "`root_j_dir'/FILEPATH"
	// directory for output on the FILEPATH drive
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on FILEPATH
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	// directory for standard code files

	// write log if running in parallel and log is not already open
	log using "`tmp_dir'/FILEPATH.smcl", replace name(master)
	
	// check for finished.txt produced by previous step
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir "FILEPATH" dirs "`step'_*", respectcase
			// remove extra quotation marks
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file "FILEPATH.txt"
			if _rc {
				di "`dir' failed"
				** BREAK
			}
		}
	}	
	
// Filepaths
	local gbd_ado "FILEPATH"
	local diag_dir "`tmp_dir'/FILEPATH"
	local dw_out_dir  "`out_dir'/FILEPATH"
	local stepfile "`code_dir'/FILEPATH.xlsx"
	
// Import functions & start timer
	adopath + "`code_dir'/ado"
	adopath + `gbd_ado'
	adopath + "FILEPATH"
	local slots 4	
	start_timer, dir("`diag_dir'") name("`step_name'") slots(`slots')

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
	
// SET LOCALS
// set memory (gb) for each job
	local mem 2
// set type for pulling different years (cod/epi); this is used for what parellel jobs to submit based on cod/epi estimation demographics, not necessarily what inputs/outputs you use
	local type "epi"
	local subnational "yes"

	local debug 0

	local nonshock 0
	local shock 1
	local tester 0

	local pull_pops 0

	if `nonshock' == 1 {
		local code "`step_name'/FILEPATH.do"
	}

	if `shock' == 1 {
		local code "`step_name'/FILEPATH.do"
	}


// Get short-term (st) DWs using custom function
local category "st"
get_ncode_dw_map, out_dir("`dw_out_dir'") category("`category'") prefix("$prefix")

** now we insheet that file and save as a stata file for easy access throughout the jobs
import delimited "`dw_out_dir'/FILEPATH.csv", delim(",") clear asdouble
save "`dw_out_dir'/FILEPATH.dta", replace

get_demographics, gbd_team(epi) clear
global location_ids `r(location_ids)'
global sex_ids `r(sex_ids)'
global age_group_ids `r(age_group_ids)'
global epi_year_ids `r(year_ids)'

get_demographics, gbd_team(cod) clear
global cod_year_ids `r(year_ids)'

if `pull_pops' {
	get_population, year_id($cod_year_ids) location_id($location_ids) sex_id($sex_ids) age_group_id($age_group_ids) clear
	save "`dw_out_dir'/FILEPATH.dta", replace
}

// parallelize by location/year/sex

if `tester' == 1 {
	global location_ids 160
	global year_ids 1995
	global epi_year_ids 1995
	global sex_ids 1
}

if `nonshock' == 1{
	foreach location_id of global location_ids {
		foreach year of global epi_year_ids {
			foreach sex of global sex_ids {
			! qsub -P proj_injuries -o FILEPATH -e FILEPATH -N stprev_`location_id'_`year'_`sex' -pe multi_slot 4 -l mem_free=8 "`code_dir'/FILEPATH.sh" "`code_dir'/`code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `year' `sex'"
			}
		}
	}
}


if `shock' == 1 {
	foreach location_id of global location_ids {
		forvalues year = 1990/2016 {
			foreach sex of global sex_ids {
			!qsub -P proj_injuries_2 -o FILEPATH -e FILEPATH -N stprev_shock_`location_id'_`year'_`sex' -pe multi_slot 4 -l mem_free=8 "`code_dir'/FILEPATH.sh" "`code_dir'/`code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `year' `sex'"
			}
		}
	}
}

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
	
// write check file to indicate step has finished
	file open finished using "`out_dir'/FILEPATH.txt", replace write
	file close finished
	
// Finish timing
	end_timer, dir("`diag_dir'") name("`step_name'")

	log close master
