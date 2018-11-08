// For the location passed in, loop through all years and get the draws for the parent cause id (in our case this is 393: STDs excluding syphilis)
// the draws are merged on location metadata, the cod proportions (from cod_data_prop) are joined on by sex and age group
// and then the draw is scaled by the proportion
// Saves draws for each cause, sex, location, and year seperately

// prep stata
clear all
set more off
set maxvar 32000
// Set up OS flexibility
// **
// **

// load shared functions
run "FILEPATH/get_draws.ado"
run "FILEPATH/get_location_metadata.ado"

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
get_location_metadata, location_set_id(35) clear
tempfile loc_meta
save `loc_meta', replace

// loop through years
// change year to 2017
forvalues year_id = 1980/2017 {
	
	// get the model draws
	get_draws, gbd_id_type(cause_id) gbd_id(`parent_cause_id') location_id(`location_id') year_id(`year_id') age_group_id(7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235) source(codem) clear
	drop cause_id
	
	// merge on the proportions
	merge m:1 location_id using `loc_meta', nogen keep(3)
	
	// expands each row time 4: once for each unique proportion from the 4 causes that feed into the parent cause:
	// syphilis, chlamydia, gonorrhea, other
	joinby `prop_index' using "FILEPATH.dta" 
	
	// apply proportions from the specific STDs (syph, chla, gono, other) from cod_data_prop
	foreach draw of varlist draw* {
		replace `draw' = `draw' * scaled
	}

	// save cod splits for all age groups to folders for each cause and sex
	levelsof cause_id, local(cause_ids) clean
	
	foreach cause of local cause_ids {
		foreach sex in 1 2 {
			preserve 
			keep if cause_id == `cause' & sex_id == `sex'
			keep draw* age_group_id
			export delimited using "FILEPATH/`location_id'_`year_id'_`sex'.csv"
			restore
		}
	}
}
