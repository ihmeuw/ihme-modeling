// prep stata
	clear all
	set more off
	set maxvar 32000

	// Set OS flexibility 
	if c(os) == "Unix" {
		local j "/home/j"
		set odbcmgr unixodbc
		local h "~"
	}
	else if c(os) == "Windows" {
		local j "J:"
		local h "H:"
	}
	sysdir set PLUS "`h'/ado/plus"

	// parse incoming syntax elements
	cap program drop parse_syntax
	program define parse_syntax
		syntax, out_dir(string) location(string) date(string)
		c_local date "`date'"
		c_local out_dir "`out_dir'"
		c_local location "`location'"
	end
	parse_syntax, `0'
	
	use "INTERPOLATE FILE", clear
	keep if location_id == `location'

	forvalues year_id = 1980/2016 {
			foreach sex in 1 2 {
				preserve
					keep if sex_id == `sex' & year_id == `year_id'
					keep draw* age_group_id
					export delimited using "`out_dir'/tmp/`location'_`year_id'_`sex'.csv", replace
				restore
			}
		}

	// write check here
	file open finished using "`out_dir'/checks/finished_loc`location'.txt", replace write
	file close finished
