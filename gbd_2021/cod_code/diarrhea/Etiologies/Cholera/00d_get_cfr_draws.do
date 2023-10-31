/// Get cholera CFR draws from DisMod ///
// The "Best" model for cholera should be the CFR model in DisMod //
// do "/FILEPATH/"

clear all
set more off

// Set J //
if c(os)=="Unix" global j "/home/j"
else global j "J:"

qui do "/FILEPATH/"

import delimited "/FILEPATH/", clear
keep if is_estimate==1 & most_detailed==1
tempfile locations
save `locations'

clear
tempfile loop
save `loop', emptyok

local year 1990 1995 2000 2005 2010 2015 2019 2020 2021 2022


foreach y in `year'{
	get_draws, gbd_id_type(modelable_entity_id) gbd_round_id(7) source(epi) year_id(`y') gbd_id(1182) decomp_step("iterative") status("best") clear
	append using `loop'
	save `loop', replace
}

forval i = 0/999 {
	local j = `i' + 1
	rename draw_`i' cf_`j'
}
merge m:1 location_id using `locations', nogen

save "/FILEPATH/", replace

