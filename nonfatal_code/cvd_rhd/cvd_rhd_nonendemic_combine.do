// Set up environment with necessary settings and locals

// Boilerplate
clear all
set more off

adopath + "FILEPATH"
  
// Pull in parameters from bash command
	local location "`1'"
		
	local out_dir FILEPATH

	capture log close
	log using FILEPATH/high_`location', replace

// Pull in epi draws for high income locations
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(3076) source(dismod) measure_ids(5 6) location_ids(`location') status(best) clear
	
// Save files to folder for 1810
	local sexes 1 2
	local years 1990 1995 2000 2005 2010 2016
	local measures 5 6
	foreach measure of local measures {
		foreach year of local years {
			foreach sex of local sexes {
				preserve
					keep if measure_id==`measure' & sex_id==`sex' & year==`year'
					outsheet age_group_id draw_* using `out_dir'/`measure'_`location'_`year'_`sex'.csv, comma replace
				restore
			}
		}
	}	

log close
