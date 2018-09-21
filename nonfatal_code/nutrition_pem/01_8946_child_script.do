**Calculate wasting  < -3 and > -2

//prep stata 
clear all
set more off
set maxvar 32000

// Set OS flexibility 
	if c(os) == "Unix" {
		local j "/home/j"
		set odbcmgr unixodbc
		local h "~"
	}
	else if c(os) == "Windows" {
		local j "J:"
		local h "H:"
	}

do "FILEPATH/get_draws.ado"

//transfer locals
args code_folder save_folder_8946 loc 

// STEP 1
//get dismod results for meid 8945 -- prevalence of WHZ < -3 SD
get_draws, gbd_id_field(modelable_entity_id) gbd_id(8945) location_ids(`loc') measure_ids(5) status(best) source(epi) clear

//rename draws
forvalues j=0/999 {
	quietly rename draw_`j' prev_`j'
}

** meid8945 now prev_`j'
tempfile meid8945
save `meid8945'

//get dismod results for meid 10558 -- childhood wasting exposure, < -2 SD
get_draws, gbd_id_field(modelable_entity_id) gbd_id(10558) location_ids(`loc') measure_ids(5) status(best) source(epi) clear

** meid10558 is draw_`j'
tempfile meid10558
save `meid10558'

merge 1:1 year_id location_id age_group_id sex_id using "`meid8945'", nogenerate keep(3)

//get and save 1608 values
forvalues j=0/999 {
	quietly replace draw_`j' = draw_`j' - prev_`j'
}



drop prev_* modelable_entity_id model_version_id measure_id location_id

forvalues j=0/999 {
	quietly replace draw_`j' = 0 if draw_`j' < 0
}



export delim using "`save_folder_8946'5_`loc'.csv", replace
