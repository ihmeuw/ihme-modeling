// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************


// PREP STATA
	clear
	set more off
	set maxvar 3200
	if c(os) == "Unix" {
		global prefix "{FILEPATH}"
		set odbcmgr unixodbc
		set mem 2g
	}
	else if c(os) == "Windows" {
		global prefix "{FILEPATH}"
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
run "{FILEPATH}/get_draws.ado"

// Perform split function
	foreach year in {YEAR IDS} {
		foreach sex in {SEX IDS} {
			foreach met in {METRIC IDS} {
				** Make non-edentulism proportions
				get_draws, gbd_id_field(modelable_entity_id) gbd_id(`edent_id') measure_ids(`met') source("{SOURCE}}") location_ids(`loc') year_ids(`year') sex_ids(`sex') age_group_ids({AGE GROUP IDS}) status(best) clear
				forval y = 0/999 {
					gen prop_`y' = (1-draw_`y')
				}
				drop draw*
				tempfile edent
				save `edent', replace
				** Read child cause to be split and multiply by non-edentulism proportions
				get_draws, gbd_id_field(modelable_entity_id) gbd_id(`split_id') measure_ids(`met') source("{SOURCE}") location_ids(`loc') year_ids(`year') sex_ids(`sex') age_group_ids({AGE GROUP IDS}) status(best) clear
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
