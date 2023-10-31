// submit prevalence and incidence extraction jobs
local username = c(username)
if c(os) == "Unix" {
	local prefix FILEPATH
	global prefix FILEPATH
	local K FILEPATH
	global K FILEPATH

    local cwd = c(pwd)
    cd "~"
    global h = c(pwd)
    cd `cwd'
	set more off
	set odbcmgr unixodbc
}
else if c(os) == "Windows" {
	local prefix FILEPATH
	global prefix FILEPATH
	local K FILEPATH
	global K FILEPATH
}
set more off
clear all
cap restore, not

set seed 12345
adopath + FILEPATH
adopath + FILEPATH

local repo = FILEPATH

// Options
local pre_nr_aggregate = `1'  // If 1 then run pre noise reduction aggregation
local noise_reduce = `2'  // If 1 then run noise reduction
local inp_int = `3'  // if 0 then set facilities to "ALL", if 1 then set facilities to "INP_ONLY"
local save_bundles = `4'

local write_dir = `5'

local add_sources = `6'
local run_id = "`7'"


use FILEPATH, clear
drop if measure != "prev"
levelsof bundle_id, l(prev_bundles)

use FILEPATH, clear
drop if measure != "inc"
levelsof bundle_id, l(inc_bundles)

// set date variables for use below
// Get date
local today = date(c(current_date), "DMY")
local year = year(`today')
local month = string(month(`today'),"%02.0f")
local day = string(day(`today'),"%02.0f")
local num_date = "`year'_`month'_`day'"

quiet run FILEPATH
	create_connection_string, server() database()
	local conn_string = r(conn_string)
	odbc load, exec(QUERY) `conn_string' clear
	saveold FILEPATH, replace
// ###
// Load map of location_ids
quiet run FILEPATH
	create_connection_string, server() database()
	local conn_string = r(conn_string)
	odbc load, exec(QUERY) `conn_string' clear
	keep if most_detailed == 1
	keep if regexm(ihme_loc_id, "USA") == 1
	drop most_detailed ihme_loc_id
	saveold FILEPATH, replace

// Load map of ME names
quiet run FILEPATH
	create_connection_string, server() database()
	local conn_string = r(conn_string)
	odbc load, exec(QUERY) `conn_string' clear
	saveold FILEPATH, replace
// ###

quiet run FILEPATH
create_connection_string, server() database()
local conn_string = r(conn_string)
local location_set_id = 35

local gbd_round_id = 4
odbc load, exec(QUERY) `conn_string' clear
saveold FILEPATH, replace


// Aggregate collapsed prevalence files by bid
if `pre_nr_aggregate' == 1 {

	// send prevalence bundles
	foreach bid of local prev_bundles {
		qsub
		sleep 50
	}
	// send incidence bundles
	foreach bid of local inc_bundles {	
		qsub
		sleep 50
	}
}


// SPLIT NOISE REDUCTION AND FILE SAVING UP INTO TWO SEPARATE PROCESSES

if `noise_reduce' == 1 {
	// Aggregate files for noise reduction
	if `inp_int ' == 0 {
		local prev_output_dir = FILEPATH
		local inc_output_dir = FILEPATH
		local facilities = "ALL"
	}
	if `inp_int' == 1 {
		local prev_output_dir = FILEPATH
		local inc_output_dir = FILEPATH
		local facilities = "INP_ONLY"
	}
	local files: dir "`prev_output_dir'" files
	clear
	tempfile all_files
	save `all_files', emptyok
	foreach file of local files {
		di "appending `file'..."
		append using "`prev_output_dir'/`file'"
		count if year == .
		if r(N) != 0 {
			di "`file' HAS MISSING YEAR!!!!"
			BREAK
		}
	}
	save FILEPATH, replace

	qsub

	local files: dir "`inc_output_dir'" files
	clear
	tempfile all_files
	save `all_files', emptyok
	foreach file of local files {
		di "appending `file'..."
		append using "`inc_output_dir'/`file'"
		count if year == .
		if r(N) != 0 {
			di "`file' HAS MISSING YEAR!!!!"
			BREAK
		}
	}

	save FILEPATH, replace

	qsub
}


exit, clear STATA
