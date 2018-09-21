	
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

	// get location ids
	run "GET_LOCATION_METADATA SHARED FUNCTION"
	get_location_metadata, location_set_id(35) clear
	tempfile ihme_locs
	save `ihme_locs', replace
	
** **************************************************************************
** FORMAT ANTENATAL CARE SYPHILIS TREAMENT DATA
** **************************************************************************

	capture mkdir "INPUTS"
		
	import delimited using "SYPHILIS_FILE", clear
	keep ghodisplay yearcode countrycode numeric comments v21 v22
	
	rename comments notes
	rename v21 notes1
	rename v22 notes2
	rename numeric mean
	rename countrycode ihme_loc_id
	rename yearcode year_id
	rename ghodisplay measure
	
	merge m:1 ihme_loc_id using `ihme_locs', keep(1 3) keepusing (location_id) nogen
	replace location_id = 416 if ihme_loc_id == "TUV"
	replace location_id = 380 if ihme_loc_id == "PLW"
	
	gen nid = 223675 
	
	replace mean = mean/100
	
	order year_id location_id ihme_loc_id mean nid measure notes notes1 notes2

	export delimited using "`out_dir'/inputs/anc_syphilis_treament_formatted.csv", replace
	
** **************************************************************************
** FORMAT ANTENATAL CARE SYPHILIS TEST DATA
** **************************************************************************

	import delimited using "SYPHILIS_TESTED", clear
	
	keep ghodisplay yearcode countrycode numeric comments v21 v22
	
	rename comments notes
	rename v21 notes1
	rename v22 notes2
	rename numeric mean
	rename countrycode ihme_loc_id
	rename yearcode year_id
	rename ghodisplay measure
	
	merge m:1 ihme_loc_id using `ihme_locs', keep(1 3) keepusing(location_id) nogen
	replace location_id = 416 if ihme_loc_id == "TUV"
	replace location_id = 380 if ihme_loc_id == "PLW"
	replace location_id = 369 if ihme_loc_id == "NRU"
	replace location_id = 393 if ihme_loc_id == "KNA"
	
	gen nid = 223673

	replace mean = mean/100
	
	order year_id location_id ihme_loc_id mean nid measure notes notes1 notes2

	export delimited using "`out_dir'/inputs/anc_syphilis_test_formatted.csv", replace
	
	