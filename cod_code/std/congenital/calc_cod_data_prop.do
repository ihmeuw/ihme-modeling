
** **************************************************************************
** PREP STATA
** **************************************************************************
	
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
		syntax, root_dir(string) date(string) out_dir(string)
		c_local out_dir "`out_dir'"
		c_local date "`date'"
		c_local root_dir "`root_dir'"
	end
	parse_syntax, `0'

	run "GET_COD_DATA SHARED FUNCTION" 

** **************************************************************************
** COMPUTE DATA PROPORTION
** **************************************************************************

	// get data from cod
	get_cod_data, cause_id(394) clear
	keep if data_type == "Vital Registration"
	keep cause_id location_id year age_group_id sex deaths
	rename (sex year) (sex_id year_id)
	save "`out_dir'/inputs/cod_data.dta", replace
	
	// aggregate globally
	collapse (sum) deaths , by(age_group_id sex_id)
	keep if age_group_id < 7
	tempfile all_syph
	save `all_syph', replace
	
	// get under 27 days
	keep if age_group_id <=3
	egen split = pc(deaths), prop
	drop deaths
	tempfile under27
	save `under27', replace

	// get sex specific denominator
	use `all_syph', clear
	keep if age_group_id <=3
	collapse (sum) deaths , by(sex_id)
	rename deaths env
	tempfile denom
	save `denom', replace

	// get over 27 days splits
	use `all_syph', clear
	drop if age_group_id <=3
	merge m:1 sex_id using `denom', nogen
	gen split = deaths/env
	keep age_group_id sex_id split
	append using `under27'
	rename split prop
	
	// export
	export delimited using "`out_dir'/models/cod_data_prop.csv", replace
	save "`out_dir'/models/cod_data_prop.dta", replace
	