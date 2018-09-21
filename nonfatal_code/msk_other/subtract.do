// Subtract fracture injuries from other musculoskeletal

clear all
set more off
set mem 2g
set maxvar 32000
if c(os) == "Unix" {
global prefix "FILEPATH"
set odbcmgr unixodbc
}
else if c(os) == "Windows" {
global prefix "FILEPATH"
}

local repo `1'
local location_id `2'
local year_id `3'
local sex_id `4'

local msk_id 2161

adopath + "FILEPATH"
adopath + "FILEPATH"

local tmp_dir "FILEPATH"
// write log if running in parallel and log is not already open
	local log_file "FILEPATH"
	log using "`log_file'", replace name(worker)

********************************** PREVALENCE ***************************************************

// Aggregate the injuries prevalence files we are subtracting for this iso3/year/sex
insheet using "FILEPATH", comma names clear
keep n_code
tempfile fracture_ncodes
save `fracture_ncodes', replace
insheet using "`repo'/FILEPATH.csv", comma names clear
keep if longterm == 1
merge m:1 n_code using `fracture_ncodes', keep(3) nogen
levelsof modelable_entity_id, l(mes)
clear
tempfile inj_prev
save `inj_prev', emptyok
foreach me_id of local mes {
	di "Getting me of `me_id' using get_draws"
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(`me_id') location_ids(`location_id') year_ids(`year_id') sex_ids(`sex_id') age_group_ids($age_group_ids) status(best) source(dismod) clear
	append using `inj_prev'
	save `inj_prev', replace
}
fastcollapse draw_*, type(sum) by(age_group_id)
rename draw_* inj_draw_*
save `inj_prev', replace

// Bring in MSK Other prevalence
// Get best model id for Other MSK
get_ids, table(measure) clear
keep if measure_name == "Prevalence"
local prev_id = measure_id 

get_draws, gbd_id_field(modelable_entity_id) gbd_id(`msk_id') location_ids(`location_id') year_ids(`year_id') sex_ids(`sex_id') age_group_ids($age_group_ids) status(latest) source(dismod) clear
keep if measure_id == `prev_id'
merge 1:1 age_group_id using `inj_prev', keep(3) nogen
gen threshold_draws_necessary = 0
forvalues i = 0/999 {
	gen threshold_draw = draw_`i' * .2
	replace draw_`i' = draw_`i' - inj_draw_`i'
	replace threshold_draws_necessary = threshold_draws_necessary + 1 if draw_`i' < threshold_draw
	replace draw_`i' = threshold_draw if draw_`i' < threshold_draw
	drop threshold_draw
}
drop inj_draw_*

// Save new draws in parent MSK Other folder
keep age_group_id draw_*
export delimited "FILEPATH", delim(",") replace

*********************************************** INCIDENCE **************************************************************

// Aggregate the injuries prevalence files we are subtracting for this iso3/year/sex
insheet using "FILEPATH", comma names clear
keep n_code
tempfile fracture_ncodes
save `fracture_ncodes', replace
insheet using "`repo'/"FILEPATH".csv", comma names clear
merge m:1 n_code using `fracture_ncodes', keep(3) nogen
levelsof modelable_entity_id, l(mes)
clear
tempfile inj_inc
save `inj_inc', emptyok
foreach me_id of local mes {
	di "Getting me of `me_id' using get_draws"
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(`me_id') location_ids(`location_id') year_ids(`year_id') sex_ids(`sex_id') age_group_ids($age_group_ids) status(best) source(dismod) clear
	append using `inj_inc'
	save `inj_inc', replace
}
keep if measure_id == 6
fastcollapse draw_*, type(sum) by(age_group_id)
rename draw_* inj_draw_*
save `inj_inc', replace

get_ids, table(measure) clear
keep if measure_name == "Incidence"
local inc_id = measure_id 

get_draws, gbd_id_field(modelable_entity_id) gbd_id(`msk_id') location_ids(`location_id') year_ids(`year_id') sex_ids(`sex_id') age_group_ids($age_group_ids) status(best) source(dismod) clear
keep if measure_id == `inc_id'
merge 1:1 age_group_id using "`inj_inc'", keep(3) nogen
gen threshold_draws_necessary = 0
forvalues i = 0/999 {
	gen threshold_draw = draw_`i' * .2
	replace draw_`i' = draw_`i' - inj_draw_`i'
	replace threshold_draws_necessary = threshold_draws_necessary + 1 if draw_`i' < threshold_draw
	replace draw_`i' = threshold_draw if draw_`i' < threshold_draw
	drop threshold_draw
}
drop inj_draw_*

// Save new draws in parent MSK Other folder
keep age_group_id draw_*
export delimited "FILEPATH", delim(",") replace

log close worker
erase "`log_file'"
di "DONE"

