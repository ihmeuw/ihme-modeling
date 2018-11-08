****************************************
****************************************
/*
This script is meant to hybridize two PAR adjusted models:
 - Brazil subnational estimates are taken from PAR estimates WITH the urban areas masked out
 - Everywhere else is taken from PAR estimates without the urban areas masked out

 */

	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}

	adopath + FILEPATH
	adopath + FILEPATH

	* get location info
	get_location_metadata, location_set_id(35) clear
	keep if is_estimate == 1 & most_detailed == 1
	tempfile locs
	save `locs', replace

	keep if regexm(ihme_loc_id, "BRA")
	levelsof location_id, local(bra_locs)

	foreach location of local bra_locs {
		import delimited "FILEPATH/`location'.csv", clear
		export delimited "FILEPATH/`location'.csv", replace
	}

	use `locs', clear
	drop if regexm(ihme_loc_id, "BRA")
	levelsof location_id, local(other_locs)

	foreach location of local other_locs {
		import delimited "FILEPATH/`location'.csv", clear
		export delimited "FILEPATH/`location'.csv", replace
	}

	* save results

save_results_epi, modelable_entity_id(2797) mark_best(TRUE) db_env("prod") input_file_pattern({location_id}.csv) description("Hybridized PAR (urban mask over BRA only)") input_dir(FILEPATH) measure_id(5) clear
