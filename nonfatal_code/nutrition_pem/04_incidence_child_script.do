* incidence child script


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
args code_folder save_folder_whz2_noedema save_folder_whz2_edema save_folder_whz3_noedema save_folder_whz3_edema loc ratio_folder


import delimited using "`ratio_folder'ratio_`loc'.csv", clear
tempfile ratios
save `ratios'

****************************************************************************************************************************
** whz3 no edema
get_draws, gbd_id_field(modelable_entity_id) gbd_id(1607) location_ids(`loc') measure_ids(5) status(best) source(epi) clear

* merge to get prev:inc ratio, multiply to calculate incidence = prev * ratio 
merge 1:1 age_group_id year_id sex_id using `ratios'
keep if _merge == 3
forvalues j=0/999 {
	quietly replace draw_`j' = draw_`j' * ratio
}

keep age_group_id sex_id year_id draw_*

export delim using "`save_folder_whz3_noedema'6_`loc'.csv", replace
*****************************************************************************************************************************
** whz3 edema
get_draws, gbd_id_field(modelable_entity_id) gbd_id(1608) location_ids(`loc') measure_ids(5) status(best) source(epi) clear

* merge to get prev:inc ratio, multiply to calculate incidence = prev * ratio 
merge 1:1 age_group_id year_id sex_id using `ratios'
keep if _merge == 3
forvalues j=0/999 {
	quietly replace draw_`j' = draw_`j' * ratio
	quietly replace draw_`j' = 0 if age_group_id > 5
}

keep age_group_id sex_id year_id draw_*

export delim using "`save_folder_whz3_edema'6_`loc'.csv", replace
******************************************************************************************************************************
** whz2 no edema
get_draws, gbd_id_field(modelable_entity_id) gbd_id(10981) location_ids(`loc') measure_ids(5) status(best) source(epi) clear

* merge to get prev:inc ratio, multiply to calculate incidence = prev * ratio 
merge 1:1 age_group_id year_id sex_id using `ratios'
keep if _merge == 3
forvalues j=0/999 {
	quietly replace draw_`j' = draw_`j' * ratio
}

keep age_group_id sex_id year_id draw_*

export delim using "`save_folder_whz2_noedema'6_`loc'.csv", replace
*****************************************************************************************************************************
** whz2 edema
get_draws, gbd_id_field(modelable_entity_id) gbd_id(1606) location_ids(`loc') measure_ids(5) status(best) source(epi) clear

* merge to get prev:inc ratio, multiply to calculate incidence = prev * ratio 
merge 1:1 age_group_id year_id sex_id using `ratios'
keep if _merge == 3
forvalues j=0/999 {
	quietly replace draw_`j' = draw_`j' * ratio
	quietly replace draw_`j' = 0 if age_group_id > 5
}

keep age_group_id sex_id year_id draw_*

export delim using "`save_folder_whz2_edema'6_`loc'.csv", replace

