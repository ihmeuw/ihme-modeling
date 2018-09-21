**Idiopathic Split: Split dismod results into idiopathic and secondary epilepsy and save draws
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
args code_folder save_folder_secondary save_folder_primary loc idiopathic_prop

//get dismod results
get_draws, gbd_id_field(modelable_entity_id) gbd_id(2403) location_ids(`loc') measure_ids(5 6) status(best) source(epi)
//rename draws
forvalues j=0/999 {
	quietly rename draw_`j' prev_`j'
}

//merge on idiopathic draws
merge m:1 year_id location_id using "`idiopathic_prop'", nogenerate keep(3)

//get and save primary values
preserve
forvalues j=0/999 {
	quietly replace draw_`j' = prev_`j' * draw_`j'
}
drop prev_* modelable_entity_id model_version_id location_id
export delim using "FILEPATH.csv", replace
restore

//get and save secondary values
forvalues j=0/999 {
	quietly replace draw_`j' = prev_`j' * (1-draw_`j')
}
drop prev_* modelable_entity_id model_version_id location_id
export delim using "FILEPATH.csv", replace





