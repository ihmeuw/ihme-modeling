	
	 
	// Set OS flexibility
	clear all
	set more off
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

	// parse incoming syntax elements
	cap program drop parse_syntax
	program define parse_syntax
		syntax, in_dir(string) cause_id(string) description(string) mark_best(string)
		c_local in_dir = "`in_dir'"
		c_local cause_id = "`cause_id'"
		c_local mark_best = "`mark_best'"
		c_local description = "`description'"
	end
	parse_syntax, `0'

	run "SAVE_RESULTS SHARED FUNCTION"
	save_results, cause_id(`cause_id') description("`description'") in_dir("`in_dir'") mark_best("`mark_best'")
	