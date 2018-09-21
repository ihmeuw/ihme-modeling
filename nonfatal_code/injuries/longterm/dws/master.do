** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
** Purpose:		Calculate country-year-Ncode specific long-term disability weight draws
** Author:		USERNAME
** Description:	Use healthcare access and quality covariate to generate a "% treated" for each country-year. 
** 				Combining this % with the Ncode-specific treated and untreated DWs, generate a custom DW for 
**				each country-year.

** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
** LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

	** prep stata
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
	local check=1
	if `check'==1 {
		local 1 FILEPATH
		local 2 FILEPATH
		local 3 "DATE"
		local 4 "01c"
		local 5 long_term_dws
		local 8 "FILEPATH"
	}	

	local gbd "gbd2016"
	local functional "_inj"

	// base directory on FILEPATH
	local root_j_dir `1'
	di "1: root_j_dir: `root_j_dir'"
	// base directory on FILEPATH
	local root_tmp_dir `2'
	di "2: root_tmp_dir: `root_tmp_dir'"
	// timestamp of current run (i.e. 2014_01_17)
	local date `3'
	di "3: date: `date'"
	// step number of this step (i.e. 01a)
	local step_num `4'
	di "4: step_num: `step_num'"
	// name of current step (i.e. first_step_name)
	local step_name `5'
	di "5: step_name: `step_name'"
	// step numbers of immediately anterior parent step (i.e. for step 2: 01a 01b 01c)
	local hold_steps `6'
	di "6: hold_steps: `hold_steps'"
	// step numbers for final steps that you are running in the current run (i.e. 11a 11b 11c)
	local last_steps `7'
	di "7: last_steps `last_steps'"
    // directory where the code lives
    local code_dir `8'

	** directory for external inputs
	local in_dir "FILEPATH"
	** directory for output on the FILEPATH drive
	local out_dir "FILEPATH"
	** directory for output on FILEPATH
	local tmp_dir "FILEPATH"
	** directory for standard code files
	
	** write log if running in parallel and log is not already open
	** log using "`out_dir'/FILEPATH.smcl", replace name(master)
	
	** check for FILEPATH.txt produced by previous step
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir "FILEPATH" dirs "`step'_*", respectcase
			** remove extra quotation marks
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file "FILEPATH.txt"
			if _rc {
				di "`dir' failed"
				BREAK
			}
		}
	}	

** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************


** Settings
	local debug 0
	local testing 0

	if missing("$check") global check 0
	
** Filepaths

	local data_dir "`tmp_dir'/FILEPATH"
	local draw_out "FILEPATH"
	cap mkdir "`draw_out'"
	local summ_out "FILEPATH"
	local diag_dir "`out_dir'/FILEPATH"
	local checkfile_dir "`tmp_dir'/FILEPATH"
	local gbd_ado "FILEPATH"
	cap mkdir "`checkfile_dir'"
	cap mkdir "`summ_out'"
	cap mkdir "`draw_out'"
	
	
** update adopath
	adopath + "`code_dir'/ado"
	adopath + "`gbd_ado'"
	
** get long-term DWs
	foreach cat in lt_u lt_t {
		get_ncode_dw_map, out_dir("`data_dir'") category("`cat'") prefix("$prefix")
		rename draw* `cat'_dw*
		tempfile `cat'
		save ``cat''
	}
	
	
** merge treated onto untreated
	merge 1:1 n_code using `lt_u', assert(match) nogen
	gen tmp = 1
	local dw_file "`data_dir'/FILEPATH.dta"
	save "`dw_file'", replace
	
	
** Create long-term DWs
	get_pct_treated, prefix("$prefix") code_dir("`code_dir'")
	gen tmp = 1
	local pct_treated_file "`data_dir'/FILEPATH.dta"
	save "`pct_treated_file'", replace
	
	get_demographics, gbd_team(epi) clear

	global location_ids `r(location_ids)'

	if `testing' == 1 {
		global location_ids 101
	}

	foreach l of global location_ids {
		local name `functional'_`step_num'_`l'
	
		!qsub -e FILEPATH -o FILEPATH -P proj_injuries -N `name' -pe multi_slot 4 "FILEPATH.sh" "`code_dir'/FILEPATH.do" "`l' `dw_file' `pct_treated_file' `checkfile_dir' `tmp_dir' `draw_out' `summ_out' `gbd_ado' `diag_dir'"
		if "`holds'" == "" local holds `functional'_`step_num'_`l'
		else local holds `holds',`functional'_`step_num'_`l'
	}

// END.
