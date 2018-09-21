
	// prep stata
	clear all
	set more off
	set maxvar 32000
	if ("`c(os)'"=="Windows") {
		local j "J:"
		local h "H:"
	}
	else {
		local j "/home/j"
		local h "~"
		set odbcmgr unixodbc
	}
	sysdir set PLUS "`h'/ado/plus"
	
	// load functions
	run "GET_DRAWS SHARED FUNCTION"
	run "GET_LOCATION_METADATA SHARED FUNCTION"
	
	// parse incoming syntax elements
	cap program drop parse_syntax
	program define parse_syntax
		syntax, root_dir(string) parent_cause_id(string) location_id(string) [prop_index(string)]
		c_local root_dir = "`root_dir'"
		c_local parent_cause_id = "`parent_cause_id'"
		c_local prop_index = "`prop_index'"
		c_local location_id = "`location_id'"
	end
	parse_syntax, `0'
	
	// get the location metadata
	get_location_metadata, location_set_id(35)
	tempfile loc_meta
	save `loc_meta', replace
	
	// loop through years
	forvalues year_id = 1980/2016 {

		// get the model draws
		get_draws, gbd_id_field(cause_id) gbd_id(`parent_cause_id') location_ids(`location_id') year_ids(`year_id') age_group_ids(7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235) source(codem) clear
		drop cause_id

		// merge on the proportions 
		merge m:1 location_id using `loc_meta', nogen keep(3)
		joinby `prop_index' using "`root_dir'/tmp/props.dta"

		// apply proportions
		foreach draw of varlist draw* {
			replace `draw' = `draw' * scaled
		}
		
		// save to folders
		levelsof cause_id, local(cause_ids) clean
		foreach cause of local cause_ids {
			foreach sex in 1 2 {
				preserve
					keep if cause_id == `cause' & sex_id == `sex'
					keep draw* age_group_id
					export delimited using "`root_dir'/tmp/`cause'/`sex'/`location_id'_`year_id'_`sex'.csv"
				restore
			}
		}
	}
