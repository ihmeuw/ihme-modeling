// Pull in Rankin scores; scale

// Prep Stata
	clear all
	set more off
	set maxvar 32767 
	
	if c(os) == "Unix" {
		global prefix "/home/j"
		set odbcmgr unixodbc
	}
	
	else if c(os) == "Windows" {
		global prefix "J:"
	}
// Add adopaths
	adopath + "FILEPATH"
	
// Locals for file paths
	local tmp_dir "FILEPATH"
	
	local locations 4 31 64 103 137 158 166


clear
tempfile master	
save `master', emptyok

foreach location of local locations {
	use `tmp_dir'/ages_`location'.dta,
	generate location_id = `location'
	capture destring location_id, replace
	append using `master'
	save `master', replace
	}

save FILEPATH/rankin_chronic.dta, replace
