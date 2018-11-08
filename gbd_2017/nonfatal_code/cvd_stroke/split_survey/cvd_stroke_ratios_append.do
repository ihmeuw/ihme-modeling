// Prep Stata
	clear all
	set more off
	set maxvar 32767

	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}

	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}
// Add adopaths
	adopath + "FILEPATH"

// Locals for file paths
	local tmp_dir "FILEPATH"
	local log_dir "FILEPATH"

// write log if running in parallel and log is not already open
	cap log close
	log using "`log_dir'/log_append.smcl", replace

// Functions
get_location_metadata, location_set_id(35)
levelsof location_id, local(location_ids)
clear

tempfile master
save `master', replace emptyok

foreach loc of local location_ids {
	use "`tmp_dir'/ratios_`loc'.dta", clear
	duplicates drop
	append using `master'
	save `master', replace
}

save "FILEPATH", replace
log close
