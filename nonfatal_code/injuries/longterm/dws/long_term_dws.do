** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
** Purpose:		Calculate year-Ncode specific DWs for a given location
** Author:		FILEPATH

** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
** LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

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
	
	di "one: `1'"
	** functional group (i.e. _inj)
	local functional "_inj"
	di "functional: `functional'"
	** gbd version (i.e. gbd2013)
	local gbd "gbd2016"
	di "gbd: `gbd'"
	** local repo
	local repo "FILEPATH"
	di "repo: `repo'"
	** iso3
	local location_id `1'
	di "location_id `location_id'"
	** file of DWs
	local dw_file `2'
	di "dw_file `dw_file'"
	** file of % treated for each country-year
	local pct_treated_file `3'
	di "pct_treated_file: `pct_treated_file'"
	** Directory for sub-step check files
	local checkfile_dir `4'
	di "checkfile_dir: `checkfile_dir'"
	** directory for output (FILEPATH)
	local tmp_dir `5'
	di "tmp_dir: `tmp_dir'"
	** Directory to save draws
	local draw_out `6'
	di "draw_out: `draw_out'"
	** Directory to save summary output
	local summ_out `7'
	di "summ_out: `summ_out'"
	** Directory of general GBD ado functions
	local gbd_ado `8'
	di "gbd_ado: `gbd_ado'"
	** Step diagnostics directory
	local diag_dir `9'
	di "diag_dir: `diag_dir'"
	** directory for steps code
	local code_dir "FILEPATH"
	** directory for external inputs
	local in_dir "FILEPATH"
	** directory for standard code files
	adopath + "FILEPATH"
	
	** write log if running in parallel and log is not already open
	local log_file "`tmp_dir'/FILEPATH.smcl"
	** log using "`log_file'", replace name(gen_dws)
	
	
** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
** Settings
	set type double, perm

** Import ado functions
	adopath + "`code_dir'/ado"
		
** Filepaths
	local out_dir = subinstr("`tmp_dir'","/FILEPATH/","FILEPATH",.)
	local data_dir "`tmp_dir'/FILEPATH"
	local diag_dir "`out_dir'/FILEPATH"

	set type double
	use "`pct_treated_file'", clear
	keep if location_id == `location_id'
	levelsof year_id, local(years)
	foreach y of local years {
		preserve
		keep if year == `y'
		merge 1:m tmp using "`dw_file'", assert(match) nogen
		drop tmp
	** Get actual dws
		forvalues x = 0/999 {
			gen draw_`x' = lt_u_dw`x' + ( pct_treated_`x' * (lt_t_dw`x' - lt_u_dw`x') )
			drop lt_u_dw`x' lt_t_dw`x' pct_treated_`x'
		}

	** Save file
		keep n_code draw_*
		rename n_code healthstate
		format draw* %16.0g
		export delimited using "`draw_out'/FILEPATH.csv", delim(",") replace
		
	** save summary file
		fastrowmean draw*, mean_var_name("name")
		fastpctile draw*, pct(2.5 97.5) names(ll ul)
		drop draw*
		export delimited using "`summ_out'/FILEPATH.csv", delim(",") replace
		
		restore
	}
