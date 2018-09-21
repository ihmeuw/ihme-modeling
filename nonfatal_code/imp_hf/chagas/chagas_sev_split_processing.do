// Pull in Chagas, split into asymp/mild, moderate, severe

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
	
// Get inputs from bash commmand
	local location "`1'"
			
// write log if running in parallel and log is not already open
	cap log close
	log using "FILEPATH/chagas_`location'.smcl", replace

// Get locations and demographic information; make macros
	//get_location_metadata, location_set_id(9)
	//get_demographics, gbd_team(epi)
	local year_ids 1990 1995 2000 2005 2010 2016
	local sex_ids 1 2 

// Import chagas envelope
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(2413) source(dismod) measure_ids(5) location_ids(`location') status(best) clear
	drop measure_id model_version_id modelable_entity_id
	forvalues j = 0/999 {
		rename draw_`j' chagas_`j'
	}
	tempfile chagas
	save `chagas', replace
	
// Process envelopes
	// Chagas
	use `chagas', clear  // updated splits from data (FILEPATH/FunctionalClassChagas_GBD2016_updates.xlsx)
	forvalues j = 0/999{
		quietly gen mild_`j' = chagas_`j'*0.862
		quietly gen moderate_`j' = chagas_`j'*0.074
		quietly gen severe_`j' = chagas_`j'*0.064
	}
	tempfile chagas_split
	save `chagas_split', replace
	
	local save_type "mild moderate severe"
	local year_ids 1990 1995 2000 2005 2010 2016
	local sex_ids 1 2 
	
	foreach kind of local save_type {
		use `chagas_split', clear
		keep age_group_id sex_id year_id `kind'_*
		forvalues j = 0/999 {
			quietly rename `kind'_`j' draw_`j'
		}
		
		foreach year_id of local year_ids {
			foreach sex_id of local sex_ids {
				preserve
				keep if sex_id==`sex_id' & year_id==`year_id'
				keep age_group_id draw_*
				outsheet using "`tmp_dir'/draws/chagas/`kind'/5_`location'_`year_id'_`sex_id'.csv", comma replace
				restore
				}
			}
		}


	
log close
