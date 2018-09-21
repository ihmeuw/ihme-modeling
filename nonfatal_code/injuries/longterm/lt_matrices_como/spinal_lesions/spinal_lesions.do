// Split spinal lesion prevalence on Frankel severity levels from literature
// USERNAME
// DATE

clear all
set more off
set mem 2g
set maxvar 32000
if c(os) == "Unix" {
	global prefix "FILEPATH"
	set odbcmgr unixodbc
}
	else if c(os) == "Windows" {
	global prefix "FILEPATH"
}

if "`1'"=="" {
	local 1 FILEPATH
	local 2 FILEPATH
	local 3 DATE
	local 4 "07"
	local 5 long_term_final_prev_and_matrices
	local 6 "FILEPATH"
	local 7 160
	local 8 2016
	local 9 1
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
// directory for standard code files
adopath + "FILEPATH"
adopath + "`code_dir'/ado"

local diag_dir "FILEPATH"

local prev_dir = "FILEPATH"

// write log if running in parallel and log is not already open
	local log_file "`tmp_dir'/FILEPATH.smcl"
	log using "`log_file'", replace name(worker)
	start_timer, dir("`diag_dir'") name("`location_id'_`year'_`sex'") slots(4)


load_params

// Load A-D severity proportion draws
local spinal_dir = "FILEPATH"
insheet using "`spinal_dir'/prop_a.csv", comma names clear
	tempfile prop_a
	save `prop_a'
insheet using "`spinal_dir'/prop_b.csv", comma names clear
	tempfile prop_b
	save `prop_b'
insheet using "`spinal_dir'/prop_c.csv", comma names clear
	tempfile prop_c
	save `prop_c'
insheet using "`spinal_dir'/prop_d.csv", comma names clear
	tempfile prop_d
	save `prop_d'	

local spinals "N33 N34"
local categories "a b c d"
foreach spinal of local spinals {
	foreach cat of local categories {
		local new_cat = "`spinal'`cat'"
		cap mkdir "`prev_dir'/`new_cat'"
		cap mkdir "`prev_dir'/`new_cat'/1"
		local prev_dir_`cat' = "`prev_dir'/`new_cat'/1"
	}
}

foreach spinal of local spinals {

	* Severity split this c/y/s/n-code
	di "Splitting `spinal' for `location_id'_`year'_`sex'"
	quietly {
		insheet using "`prev_dir'/`spinal'/1/5_`location_id'_`year'_`sex'.csv", comma names clear
		foreach cat of local categories {
			preserve
			gen acause = "_none"
			merge m:1 acause using `prop_`cat''
			forvalues i = 0/999 {
				replace draw_`i' = draw_`i' * prop_draw_`i'
			}
			drop _merge prop_draw_* acause
			outsheet using "`prev_dir'/`spinal'`cat'/1/5_`location_id'_`year'_`sex'.csv", comma names replace
			restore
		}
	}
	* End severity split for this c/y/s/n-code

}

// End timer
	end_timer, dir("`diag_dir'") name("`location_id'_`year'_`sex'")
		
// End log and erase if not debugging
	log close worker
	erase "`log_file'"

di "Done"
* END
