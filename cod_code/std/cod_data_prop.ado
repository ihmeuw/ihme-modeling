
/*
local cause_ids 394 395 396 399
395
local group_by = "sex_id age_group_id"
*/

cap program drop cod_data_prop
program define cod_data_prop
	syntax , cause_ids(string) [group_by(string)] [clear]

	// Set OS flexibility
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

	// load in necessary shared functions
	run "GET_COD SHARED FUNCTION"
	run "GET_LOCATION_METADATA SHARED FUNCTION"

	// get data from cod
	clear
	tempfile appended
	save `appended', replace emptyok
	foreach cause of local cause_ids {
	get_cod_data, cause_id(`cause') clear
	append using `appended'
	save `appended', replace
	}
	keep if data_type == "Vital Registration"
	keep cause_id location_id year age_group_id sex study_deaths sample_size
	rename (sex year) (sex_id year_id)
	tempfile causes
	save `causes', replace
	// merge on location metadata
	get_location_metadata, location_set_id(35) clear
	merge 1:m location_id using `causes', keep(3)
	
	// genereate props
	replace study_deaths = 0 if cause_id == 395 & sex_id == 1  // chlamydial deaths set to 0
	collapse (sum) study_deaths sample_size , by(cause_id `group_by')
	gen prop = study_deaths/sample_size
	if "`group_by'" != "" bysort `group_by': egen scaled = pc(prop), prop
	if "`group_by'" == "" egen scaled = pc(prop), prop
	drop study_deaths sample_size prop

end





