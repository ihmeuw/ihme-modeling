// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author: 	NAME
// Date: 		04 August 2014
// Purpose:	Reduce periodental and permanent caries to reflect only non-edentulism
// do "FILEPATH"

// PREP STATA
	clear
	set more off
	set maxvar 3200
	if c(os) == "Unix" {
		global prefix "ADDRESS"
		set odbcmgr unixodbc
		set mem 2g
	}
	else if c(os) == "Windows" {
		global prefix "ADDRESS"
		set mem 2g
	}

// Temp directory
	local tmp_dir "`1'"

// ME_id of edentulism
	local edent_id `2'

// ME_id of results
	local split_id `3'

// Location_id
	local loc `4'


// ****************************************************************************
// Log work
	capture log close
	log using "`tmp_dir'/`split_id'/00_logs/`loc'_draws.smcl", replace

// ****************************************************************************
// Load in necessary function
run "FILEPATH"

// Perform split function
	foreach year in 1990 1995 2000 2005 2010 2015 2019 2020 2021 2022{
	**foreach year in 2015 2019 {
		foreach sex in 1 2 {
			foreach met in 5 6 {
				** Make non-edentulism proportions
				get_draws, gbd_id_type("modelable_entity_id") gbd_id(`edent_id') measure_id(`met') source("epi") location_id(`loc') year_id(`year') sex_id(`sex') age_group_id(2 3 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 34 235 238 388 389) status(best) gbd_round_id(7) decomp_step("iterative") clear
								drop modelable_entity_id

				forval y = 0/999 {
					gen prop_`y' = (1-draw_`y')
				}
				drop draw*
				tempfile edent
				save `edent', replace
				** Read child cause to be split and multiply by non-edentulism proportions
				get_draws, gbd_id_type("modelable_entity_id") gbd_id(`split_id') measure_id(`met') source("epi") location_id(`loc') year_id(`year') sex_id(`sex') age_group_id(2 3 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 34 235 238 388 389) status(best) gbd_round_id(7) decomp_step("iterative") clear
												drop modelable_entity_id
				merge 1:1 age_group_id using `edent', keep(1 3) nogen
				forval z = 0/999 {
					replace prop_`z' = 1 if prop_`z' == .
					replace draw_`z' = draw_`z'*prop_`z'
				}
				drop prop* model_version_id
				outsheet using "`tmp_dir'/`split_id'/01_draws/`met'_`loc'_`year'_`sex'.csv", comma names replace
			}
		}
	}


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
