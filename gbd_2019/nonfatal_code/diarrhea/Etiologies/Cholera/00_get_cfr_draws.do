/// Get cholera CFR draws from DisMod ///
// The "Best" model for cholera should be the CFR model in DisMod //
// do "/filepath/00_get_cfr_draws.do"

clear all
set more off

// Set J //
global j "filepath"

qui do "/filepath/get_draws.ado"

import delimited "filepath", clear
keep if is_estimate==1 & most_detailed==1
tempfile locations
save `locations'

clear
tempfile loop
save `loop', emptyok

local year 1990 1995 2000 2005 2010 2015 2017 2019
// local year 1990 2000 2017

foreach y in `year'{
	get_draws, gbd_id_type(modelable_entity_id) source(epi) year_id(`y') gbd_id(1182) decomp_step("step2") clear
	append using `loop'
	save `loop', replace
}

forval i = 0/999 {
	local j = `i' + 1
	rename draw_`i' cf_`j'
}
merge m:1 location_id using `locations', nogen

save "/filepath/cholera_cf_draws.dta", replace

// If annual draws are required, uncomment the following lines //
/*
use "filepath/cholera_cf_draws.dta", clear
drop row

tempfile data
save `data'

keep if year_id==2017
expand 3
bysort location_id sex_id age_group_id: gen row = _n

replace year_id = year_id - row + 1
drop row

tempfile newest
save `newest'

use `data', clear
keep if year_id != 2017
append using `newest'

tab year_id
keep if year_id <= 2017

save "filepath/cholera_cf_draws.dta", replace
*/
