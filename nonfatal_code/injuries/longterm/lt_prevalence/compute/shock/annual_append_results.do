// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		Append annual results for the prevalence ODE solver for shocks
// Author:		USERNAME
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "FILEPATH" {
		global prefix "FILEPATH"
	}
	
	// base directory on J 
	local root_j_dir `1'
	// base directory on clustertmp
	local root_tmp_dir `2'
	// timestamp of current run (i.e. 2014_01_17)
	local date `3'
	// step number of this step (i.e. 01a)
	local step_num `4'
	// name of current step (i.e. first_step_name)
	local step_name `5'
    // directory where the code lives
    local code_dir `6'
    // iso3
	local location_id `7'
	// year
	local year `8'
	// sex
	local sex `9'
	// directory for external inputs
	local in_dir "`root_j_dir'/FILEPATH"
	// directory for output on the J drive
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on clustertmp
	local tmp_dir "`root_tmp_dir'/FILEPATH"

	local gbd_ado "$FILEPATH"

	// write log if running in parallel and log is not already open
	local log_file "`tmp_dir'/FILEPATH.smcl"
	log using "`log_file'", replace name(worker)
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// Filepaths
	
	local diag_dir "FILEPATH"
	local prev_results_dir "FILEPATH"
	local output_dir "`tmp_dir'/FILEPATH"
	local draw_dir "`output_dir'/FILEPATH"
	local summ_dir "`output_dir'/FILEPATH"

// Settings
	local slots 1
	local debug 99
	set type double, perm

// Import GBD functions
	adopath + `gbd_ado'
	adopath + `code_dir'/ado

// Get iso3
	get_location_metadata, location_set_id(35) clear
	keep if location_id == `location_id'
	local iso3 = [ihme_loc_id]

// Load injury parameters
	load_params
	
// Load file of zero draws to copy where missing location/year/sex
insheet using "`in_dir'/FILEPATH.csv", comma names clear
tempfile zero_draws
save `zero_draws', replace

// Load ncodes 
insheet using "`code_dir'/FILEPATH.csv", comma names clear
levelsof n_code, l(ncodes)

local shock_codes inj_war_warterror inj_war_execution inj_disaster

// Pull shocks results and append 
	if `sex' == 1 {
		local sex_string = "male"
	}
	if `sex' == 2 {
		local sex_string = "female"
	}
	clear
	tempfile shocks_appended
	local shocks_dir = "FILEPATH"
	local platforms inp otp 
	local count 0
	foreach plat of local platforms {
		foreach ecode of local shock_codes {
			foreach ncode of local ncodes {
				local count = `count' + 1
			}
		}
	}
	local counter 0
	foreach plat of local platforms {
		foreach ecode of local shock_codes {
			foreach ncode of local ncodes {
					di "`counter'/`count'"
					quietly {
						cap import delimited "FILEPATH.csv", asdouble clear
						if _rc {
							use `zero_draws', clear
						}
						gen e_code = "`ecode'"
						gen n_code = "`ncode'"
						if "`plat'" == "inp" {
							gen inpatient = 1 
						}
						if "`plat'" == "otp" {
							gen inpatient = 0
						}
						cap confirm file `shocks_appended'
						if _rc == 0 append using `shocks_appended'
						save `shocks_appended', replace
					}
					local counter = `counter' + 1
				}
			}
		}
	

// Format
	order e_code n_code inpatient age, first
	sort_by_ncode n_code, other_sort(inpatient age)
	sort e_code
	format draw* %16.0g
	
// Save draws
	local outfile_name prevalence_`location_id'_`year'_`sex'.csv
	export delimited "`draw_dir'/FILEPATH", replace
	
	tempfile smrs_draws
	save `smrs_draws', replace
	
// Save summary stats
	fastrowmean draw*, mean_var_name("mean")
	fastpctile draw*, names(ll ul) pct(2.5 97.5)
	export delimited e_code n_code inpatient age mean ll ul using "`summ_dir'/`outfile_name'", replace
	
	keep e_code n_code inpatient age mean ll
	tempfile smrs_sum
	save `smrs_sum', replace

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)
