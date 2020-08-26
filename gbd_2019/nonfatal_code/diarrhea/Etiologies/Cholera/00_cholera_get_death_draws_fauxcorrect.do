/// Pulls draws for diarrhea mortality post CoDCorrect ///
// This is required for the cholera mortality etiology attribution //
// do "/filepath/00_cholera_get_death_draws_dismod.do"

clear all
set more off

set maxvar 8000
** Set directories
	global j "filepath"
	//set odbcmgr unixodbc

local username USERNAME
qui do "/filepath/get_draws.ado"
import delimited "/filepath/ihme_loc_metadata_2019.csv", clear
keep if most_detailed==1
keep if is_estimate==1
levelsof location_id, local(locs)

clear
tempfile draws
save `draws', emptyok

local year 1990 1995 2000 2005 2010 2015 2017 2019
local year 1990 2000 2017

foreach y in `year' {
	di "`y'"
	get_draws, gbd_id_type(cause_id) source(fauxcorrect) gbd_id(302) year_id(`y') location_id(`locs') version_id(1) gbd_round_id(6) decomp_step("step2") clear
	append using `draws'
	save `draws', replace
}

save "/filepath/death_draws_fauxcorrect.dta", replace

// If annual draws are required, uncomment. //
/*
use "filepath/death_draws_annual.dta", clear
expand 5
bysort year_id location_id sex_id age_group_id: gen row = _n
replace year_id = year_id + row - 1
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

save "filepath/death_draws.dta", replace
*/
