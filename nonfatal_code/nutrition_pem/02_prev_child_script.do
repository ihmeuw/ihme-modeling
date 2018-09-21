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
args code_folder save_folder_whz2_noedema save_folder_whz2_edema save_folder_whz3_noedema save_folder_whz3_edema loc 

// step 0 
// dismod results for 175328 to get prevalence estimates from multiple parameter model, for use in scaling later on
get_draws, gbd_id_field(modelable_entity_id) gbd_id(10558) location_ids(`loc') measure_ids(5) status(best) source(epi) model_version_id(175328) clear

forvalues j=0/999 {
	quietly rename draw_`j' c_prev_`j'
}

tempfile meid111641
save `meid111641'


// STEP 1
//get dismod results for meid 10553
get_draws, gbd_id_field(modelable_entity_id) gbd_id(10553) location_ids(`loc') measure_ids(18) status(best) source(epi) clear

//rename draws
forvalues j=0/999 {
	quietly rename draw_`j' prev_`j'
}

** meid10553 now prev_`j'
tempfile meid10553
save `meid10553'

//get dismod results for meid 8945
get_draws, gbd_id_field(modelable_entity_id) gbd_id(8945) location_ids(`loc') measure_ids(5) status(best) source(epi) clear

** meid8945 is draw_`j'
tempfile meid8945
save `meid8945'

merge 1:1 year_id age_group_id location_id sex_id using "`meid10553'", nogenerate keep(3)

//get and save 1608 values
forvalues j=0/999 {
	quietly replace draw_`j' = prev_`j' * draw_`j'
	quietly replace draw_`j' = 0 if age_group_id > 5
}

forvalues j=0/999 {
	quietly replace draw_`j' = 0 if draw_`j' < 0
}

preserve

drop prev_* modelable_entity_id model_version_id measure_id location_id
export delim using "`save_folder_whz3_edema'5_`loc'.csv", replace

restore
drop prev_*

forvalues j=0/999 {
	quietly rename draw_`j' prev_`j'
}

** meid1608 now prev
tempfile meid1608
save `meid1608'

***********************************************************************************************************************
// STEP 2
merge 1:1 year_id age_group_id location_id sex_id using "`meid8945'", nogenerate keep(3)

//get and save 1607 values 
forvalues j=0/999 {
	  quietly replace draw_`j' = draw_`j' - prev_`j'
}
drop prev_* modelable_entity_id model_version_id measure_id

forvalues j=0/999 {
	quietly replace draw_`j' = 0 if draw_`j' < 0
}

forvalues j=0/999 {
	quietly rename draw_`j' a_prev_`j'
}


tempfile meid1607
save `meid1607'

************************************************************************************************************************
// STEP 3
// get dismod results for meid 10554
get_draws, gbd_id_field(modelable_entity_id) gbd_id(10554) location_ids(`loc') measure_ids(18) status(best) source(epi) clear
merge 1:1 year_id age_group_id location_id sex_id using "`meid10553'", nogenerate keep(3)

// subtract 10554 from 10553 to get and tempfile 10555 values
forvalues j=0/999 {
	quietly replace prev_`j' = prev_`j' - draw_`j' 
}

tempfile meid10555
save `meid10555' 
**************************************************************************************************************************
// Step 4
// get dismod results for meid 8946
get_draws, gbd_id_field(modelable_entity_id) gbd_id(8946) location_ids(`loc') measure_ids(5) status(best) source(epi) clear
tempfile meid8946
save `meid8946'

merge 1:1 year_id age_group_id sex_id using "`meid10555'", nogenerate keep(3)

** get meid1606
forvalues j=0/999 {
	quietly replace draw_`j' = prev_`j' * draw_`j'
}


forvalues j=0/999 {
	quietly replace draw_`j' = 0 if draw_`j' < 0
	quietly replace draw_`j' = 0 if age_group_id > 5
}

preserve

drop prev_* modelable_entity_id model_version_id measure_id location_id

export delim using "`save_folder_whz2_edema'5_`loc'.csv", replace

restore

tempfile meid1606
save `meid1606' 

*****************************************************************************************************
// Step 5
use `meid8946'
forvalues j=0/999 {
	quietly rename draw_`j' prev_`j'
}
save `meid8946', replace

merge 1:1 year_id location_id age_group_id sex_id using "`meid1606'", nogenerate keep(3) 


forvalues j=0/999 {
	quietly replace draw_`j' = prev_`j' - draw_`j' 
}

drop prev_* modelable_entity_id model_version_id measure_id 

forvalues j=0/999 {
	quietly replace draw_`j' = 0 if draw_`j' < 0
}


forvalues j=0/999 {
	quietly rename draw_`j' b_prev_`j'
}

merge 1:1 year_id age_group_id location_id sex_id using "`meid1607'", nogenerate keep(3)
merge 1:1 year_id age_group_id location_id sex_id using "`meid111641'", nogenerate keep(3)

forvalues j=0/999 {
	quietly gen final_a_`j' = c_prev_`j' * (a_prev_`j' / (a_prev_`j' + b_prev_`j')) if age_group_id > 5
	quietly replace final_a_`j' = a_prev_`j' if age_group_id <= 5
}

forvalues j=0/999 {
	quietly gen final_b_`j' = c_prev_`j' * (b_prev_`j' / (a_prev_`j' + b_prev_`j')) if age_group_id > 5
	quietly replace final_b_`j' = b_prev_`j' if age_group_id <= 5
}

preserve
 
keep age_group_id sex_id year_id final_a*
forvalues j=0/999 {
	quietly rename final_a_`j' draw_`j'
}

export delim using "`save_folder_whz3_noedema'5_`loc'.csv", replace

restore

keep age_group_id sex_id year_id final_b*
forvalues j=0/999 {
	quietly rename final_b_`j' draw_`j'
}

export delim using "`save_folder_whz2_noedema'5_`loc'.csv", replace 


