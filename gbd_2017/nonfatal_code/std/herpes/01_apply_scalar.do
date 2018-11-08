// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author: 	
// Date: 		04 August 2014
// Updated 
// Purpose:	Apply proportion

// PREP STATA
	clear
	set more off
	// SET UP OS FLEXIBILITY

// Temp directory
	local tmp_dir "`1'"

// model_version_id
	local parent_model `2'

// Location_id
	local loc `3'

// ****************************************************************************
// Log work
	capture log close
	log using "`tmp_dir'/FILEPATH/`loc'_scaling.smcl", replace

// Load in necessary function
	run "FILEPATH/get_draws.ado"

// Load Herpes incidence and produce symptomatic
	foreach year in 1990 1995 2000 2005 2010 2017 {
		foreach sex in 1 2 {
			get_draws, gbd_id_type(modelable_entity_id) gbd_id(1642) source("epi") measure_id(6) location_id(`loc') year_id(`year') sex_id(`sex') age_group_id(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235) version_id(`parent_model') clear
			drop modelable_entity_id
			gen mvar = 1
			//don't do anything to incidence
			outsheet using "`tmp_dir'/FILEPATH/6_`loc'_`year'_`sex'.csv", comma names replace
			// merge the undiagnosed scalar that was created in 00_master
			merge m:1 mvar using "`tmp_dir'/prop_draws.dta", assert(3) nogen
			// multiply incidence by the scalar to get only the diagnosed prevalent cases
			forval t = 0/999 {
				replace draw_`t' = draw_`t'*prop_`t'
			}
			drop prop*
			replace measure_id = 5
			outsheet using "`tmp_dir'/FILEPATH/5_`loc'_`year'_`sex'.csv", comma names replace
		}
	}

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
