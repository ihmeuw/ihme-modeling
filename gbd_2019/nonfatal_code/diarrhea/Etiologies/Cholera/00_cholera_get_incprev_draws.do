/// This pulls the incidence and prevalence draws from the diarrhea DisMod models ///
// These estimates are required for the non-fatal cholera PAF estimates //
// Must be run on cluster (get_draws) 
// do "/filepath/00_cholera_get_incprev_draws.do"

clear all
set more off

set maxvar 8000

qui do "/filepath/get_draws.ado"
import delimited "/filepath/ihme_loc_metadata_2019.csv", clear
keep if level>=3
levelsof location_id, local(locs)	

/// Incidence and Prevalence all at once! ///
clear
tempfile draws
save `draws', emptyok

local year 1990 1995 2000 2005 2010 2015 2017 2019

// Currently pulls draws by year, saves, then appends the DTA files. //
foreach y in `year' {
	get_draws, gbd_id_type(modelable_entity_id) source(epi) gbd_id(1181) year_id(`y') location_id(`locs') measure_id(5 6) decomp_step("step4") clear
	save "/filepath/inc_prev_`y'.dta", replace
	save "/filepath/inc_prev_`y'.dta", replace

}

clear
cd "/filepath/"


local allfiles : dir . files "*.dta"

foreach file in `allfiles' {
    append using "`file'", force
}

// save "/filepath/inc_prev_draws.dta", replace
// save "/filepath/inc_prev_draws.dta", replace

save "/filepath/inc_prev_draws_step3.dta", replace
