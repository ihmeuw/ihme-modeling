// submit prevalence and incidence extraction jobs
local username = c(username)
if c(os) == "Unix" {
	local prefix "FILENAME"
	global prefix "FILENAME"
	local K "FILENAME"
	global K "FILENAME"

    local cwd = c(pwd)
    cd "~"
    global h = c(pwd)
    cd `cwd'
	set more off
	set odbcmgr unixodbc
}
else if c(os) == "Windows" {
	local prefix "FILEPATH"
	global prefix "FILEPATH"
	local K "K:"
	global K "K:"
}
set more off
clear all
cap restore, not
cap set maxvar 20000
set seed 12345
adopath + "`prefix"FILEPATH"
adopath + "`prefix"FILEPATH"
//local root "`prefix"FILEPATH"
local repo = "FILEPATH"

// Options
local pre_nr_aggregate = `1'  // If 1 then run pre noise reduction aggregation
local noise_reduce = `2'  // If 1 then run noise reduction
local inp_int = `3'  // if 0 then set facilities to "ALL", if 1 then set facilities to "INP_ONLY"
local save_bundles = `4' // unused

// set write_dir to either test or work
// test == 0, work == 1
local write_dir = `5' // unused

local add_sources = `6' // unused
local run_id = "`7'" // run_id for use in building filepaths for input/output

//use "FILEPATH", clear
use "FILEPATH", clear
drop if measure != "prev"
levelsof bundle_id, l(prev_bundles)

//use "FILEPATH", clear
use "FILEPATH", clear
drop if measure != "inc"
levelsof bundle_id, l(inc_bundles)

// set date variables for use below
// Get date
local today = date(c(current_date), "DMY")
local year = year(`today')
local month = string(month(`today'),"%02.0f")
local day = string(day(`today'),"%02.0f")
local num_date = "`year'_`month'_`day'"
//local num_date 2018_05_24
//local date may_24_2018

// only used when writing data to J WORK
//local ms_vers "GBD2017_v8_TWN_MS2013"

// 
// Load acause
// use the id to acause map to get the REIs

/*

NOTE

The files all exist from the last time claims was ran.  And the files don't really change between runs

*/
quiet run "FILEPATH"
	create_connection_string, server("DATABASE") database("DATABASE")
	local conn_string = r(conn_string)
	odbc load, exec(QUERY)
	saveold "FILEPATH", replace
// 
// Load map of location_ids
quiet run "FILEPATH"
	create_connection_string, server("DATABASE") database("DATABASE")
	local conn_string = r(conn_string)
	odbc load, exec(query)
	keep if most_detailed == 1
	keep if regexm(ihme_loc_id, "USA") == 1
	drop most_detailed ihme_loc_id
	saveold "FILEPATH", replace

// Load map of ME names
quiet run "FILEPATH"
	create_connection_string, server("DATABASE") database("DATABASE")
	local conn_string = r(conn_string)
	odbc load, exec(QUERY)
	saveold "FILEPATH", replace
// 

// save flat files upfront to avoid multiple db quieries
quiet run "FILEPATH"
create_connection_string, server("DATABASE") database("DATABASE")
local conn_string = r(conn_string)
local location_set_id = 35
// Yes, this is out of date, but, this is a house of cards.
local gbd_round_id = 4
odbc load, exec(query)
saveold "FILEPATH", replace




// Aggregate collapsed prevalence files by bid
if `pre_nr_aggregate' == 1 {
	/*
	// clear out all the existing files
	foreach childdir in inc_pre_noise_reduction_marketscan_all inc_pre_noise_reduction_marketscan_all_inp prev_pre_noise_reduction_marketscan_all prev_pre_noise_reduction_marketscan_all_inp {
		local datafiles: dir "FILEPATH"
		foreach datafile of local datafiles {
			!rm "FILEPATH"
		}
	}
	*/

	// send prevalence bundles
	foreach bid of local prev_bundles {
		!qsub -o FILEPATH -e FILEPATH -P proj_hospital -N pre_nr_`bid'_`run_id' -l archive -l fthread=4 -l m_mem_free=4G -l h_rt=2:00:00 -q all.q "FILEPATH" "`repo'/01c_msdb_aggregate_prevalence_nocollapse.do" "`bid' `run_id'"
		sleep 50
	}
	// send incidence bundles
	foreach bid of local inc_bundles {	
		!qsub
		sleep 50
	}
}


// SPLIT NOISE REDUCTION AND FILE SAVING UP INTO TWO SEPARATE PROCESSES

if `noise_reduce' == 1 {
	// Aggregate files for noise reduction
	if `inp_int' == 0 {
		//local prev_output_dir = "FILENAME"
		//local inc_output_dir = "FILENAME"
		local prev_output_dir = "FILEPATH"
		local inc_output_dir = "FILEPATH"
		local facilities = "ALL"
	}
	if `inp_int' == 1 {
		//local prev_output_dir = "FILENAME"
		//local inc_output_dir = "FILENAME"
		local prev_output_dir = "FILEPATH"
		local inc_output_dir = "FILEPATH"
		local facilities = "INP_ONLY"
	}
	local files: dir "FILEPATH"
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

	// drop year 2013 because it's not good (no inp dates)
	// drop if year == 2013

	save "FILEPATH", replace

	!qsub

	local files: dir "FILEPATH"
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

	// drop year 2013 because it's not good (no inp dates)
	// drop if year == 2013

	save "FILEPATH", replace

	!qsub
}

exit, clear STATA
