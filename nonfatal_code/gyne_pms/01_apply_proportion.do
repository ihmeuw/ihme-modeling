// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author: 	Ryan Barber
// Date: 		04 August 2014
// Edited by Katie Thomas for GBD 2016 13 February 2017
// Purpose:	Apply proportion


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


*
	// Temp directory
	local tmp_dir "`1'"

// location id
	local loc `2'

// Parent pms me_id
	local pms_id `3'

// Proportion me_id
  local prop_id `4'
*/  


// ****************************************************************************
// Log work
	capture log close
	log using "`tmp_dir'/00_logs/`loc'_preg_proportion.smcl", replace

	// Load in necessary function
    run "{FILEPATH}/get_draws.ado"

// Produce symptomatic
	foreach year in {YEAR IDS} {
		foreach sex in {SEX IDS} {
			// for incidence:
			// pull in incidence draws for the parent pms model
			get_draws, gbd_id_field(modelable_entity_id) gbd_id(`pms_id') source({SOURCE})measure_ids({MEASURE ID}) location_ids(`loc') year_ids(`year') sex_ids(`sex') age_group_ids({AGE GROUP IDS}}) status(best) clear
			renpfix draw case
			tempfile cases
			save `cases', replace
			// pull in incidence draws for the pregnancy proportion model
			get_draws, gbd_id_field(modelable_entity_id) gbd_id(`prop_id') source({SOURCE})measure_ids({MEASURE ID}) location_ids(`loc') year_ids(`year') sex_ids(`sex') age_group_ids({AGE GROUP IDS}) status(best) clear
			renpfix draw prop
			merge 1:1 age_group_id using `cases', keep(3) nogen
			// modify the parent by multiplying by the non-pregnant draws from the pregnancy propotion model
			forval t = 0/999 {
				gen draw_`t' = case_`t'*(1-prop_`t')
			}
			keep draw* age_group_id
			outsheet using "`tmp_dir'/01_draws/6_`loc'_`year'_`sex'.csv", comma names replace
			// do this all again for prevalence
			get_draws, gbd_id_field(modelable_entity_id) gbd_id(`pms_id') source({SOURCE})measure_ids({MEASURE ID}) location_ids(`loc') year_ids(`year') sex_ids(`sex') age_group_ids({AGE GROUP IDS}) status(best) clear
			renpfix draw case
			tempfile cases
			save `cases', replace
			get_draws, gbd_id_field(modelable_entity_id) gbd_id(`prop_id') source({SOURCE}})measure_ids({MEASURE ID}) location_ids(`loc') year_ids(`year') sex_ids(`sex') age_group_ids({AGE GROUP IDS}) status(best) clear
			renpfix draw prop
			merge 1:1 age_group_id using `cases', keep(3) nogen
			forval t = 0/999 {
				gen draw_`t' = case_`t'*(1-prop_`t')
			}
			keep draw* age_group_id
			outsheet using "`tmp_dir'/01_draws/5_`loc'_`year'_`sex'.csv", comma names replace
		}
	}

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
