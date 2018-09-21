**Severe Split: Split dismod results into severe and less severe epilepsy and save draws 
**2/17/2017

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
args code_folder save_folder_severe save_folder_notsevere save_folder_tnf loc severe_prop tg_prop tnf_prop

//get dismod results
di `loc'
get_draws, gbd_id_field(modelable_entity_id) gbd_id(3025) location_ids(`loc') measure_ids(5 6) status(best) source(epi) 
//rename draws
forvalues j=0/999 {
	quietly rename draw_`j' prev_`j'
}

//merge on idiopathic draws
merge m:1 year_id location_id using "`severe_prop'", nogenerate keep(3)
preserve

//get and save severe values
forvalues j=0/999 {
	quietly replace draw_`j' = prev_`j' * draw_`j'
}
drop prev_* modelable_entity_id model_version_id
export delim using "FILEPATH.csv", replace

//get less severe draws
restore
forvalues j=0/999 {
	quietly gen not_severe_`j' = prev_`j' * (1-draw_`j')
}
drop prev_* draw_* modelable_entity_id model_version_id

//merge on treatment gap draws
merge m:1 year_id location_id using "`tg_prop'", nogenerate keep(3)

//get not severe treated draws
forvalues j=0/999 {
	quietly gen treated_`j' = not_severe_`j' * (1-draw_`j')
}
drop draw_*

//merge on treated no fits draws
merge m:1 year_id location_id using "`tnf_prop'", nogenerate keep(3)

//get values treated no fits
forvalues j=0/999 {
	quietly gen tnf_`j' = treated_`j' * draw_`j'
}
drop draw_* treated_* 

//save tnf
preserve
drop not_severe_*
forvalues j=0/999 {
	quietly rename tnf_`j' draw_`j'
}
export delim using "FILEPATH.csv", replace

//calculate not severe as not severe - tnf
restore
forvalues j=0/999 {
	quietly replace not_severe_`j' = not_severe_`j' - tnf_`j'
} 
drop tnf_*

//save not severe
forvalues j=0/999 {
	quietly rename not_severe_`j' draw_`j'
}
export delim using "FILEPATH.csv", replace

