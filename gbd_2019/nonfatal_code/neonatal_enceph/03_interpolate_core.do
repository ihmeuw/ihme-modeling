
// priming the working environment
clear all
set more off
set maxvar 30000
version 13.0

// discover root 
		if c(os) == "Windows" {
			local j "J:"
		}
		if c(os) == "Unix" {
			local j "FILEPATH/j"
		} 

// arguments
local me_id `1'
local acause `2'
local location_id `3'
local year_id `4'
local sex_id `5'

// functions
adopath + FILEPATH_to_function
run FILEPATH get_draws.ado

// directories
local data_dir "FILEPATH"

*******************************************************************

// import data for this location-year-sex
di "Location = `location_id', year = `year_id', sex = `sex_id', me_id = `me_id', acause = `acause'"
get_draws, gbd_id_type(modelable_entity_id) gbd_id(`me_id') source(epi) measure_id(5) location_id(`location_id') year_id(`year_id') age_group_id(2 3) sex_id(`sex_id') gbd_round_id(6) status(best) decomp_step(step4) clear

/* "Match" draws from age_group_id 3 to age_group_id 2. This is different than just sorting both rows smallest to largest. 
We're still matching each draw for age_group 3 with it's corresponding partner in magnitude in age_group_2, 
but we're preserving the original order of the draws in age_group 2. This is so we don't wind up with all of our lowest value 
draws in the draw_0 column, and all of our highest values in draw_999. Because that is problematic during aggregation (say, 
from the US state to the national level) - aggregation is just a weighted average, so if you line up all your smallest and 
largest values, you'll get inflated uncertainty bounds. */

	// reshape long
	reshape long draw_, i(age_group_id) j(draw_num)
	rename draw_ prevalence

	// generate a 'target_order' variable. This preserves the original order of the draws. 
	bysort age_group_id: gen target_order = _n

	// generate a `sort_order' variable. This contains the order smallest -> largest of prevalence values for each age_group. 
	sort age_group_id prevalence
	bysort age_group_id: gen sort_order = _n

	// make "template"
	preserve
	keep if age_group_id == 2 
	rename prevalence age_group_id_2
	tempfile template
	save `template', replace
	restore

	// format that which you will merge on the template
	preserve
	keep if age_group_id == 3
	rename prevalence age_group_id_3
	drop target_order draw_num
	tempfile stamp
	save `stamp', replace
	restore, not
	
	// merge `template' and `stamp'
	merge 1:1 sort_order using `template'
	drop _merge age_group_id sort_order
	sort target_order

	// first reshape, to prep for interpolation
	reshape long age_group_id_, i(draw_num) j(age_group_id) 
	rename age_group_id_ prevalence

	// make room for our interpolated values to come
	expand 2 if age_group_id == 3, gen(expanded)
	replace age_group_id = 99 if expanded == 1
	replace prevalence = . if expanded == 1
	drop expanded
	expand 2 if age_group_id == 2, gen(expanded)
	replace age_group_id = 164 if expanded == 1
	replace prevalence = . if expanded == 1
	drop expanded

/////////////////////
// interpolate
/////////////////////

	// create age (not age_group_id) var
	gen age = 3/365 
	// 0-6d
	
	replace age = 0 if age_group_id == 164 
	// at birth
	
	replace age = (27-7)/(365) if age_group_id == 3 
	// 7-27d
	
	replace age = 28/365 if age_group_id == 99
	// 28d 

	// interpolate 28 day
	sort draw_num age_group_id
	ipolate prevalence age, by(draw_num) generate(iprevalence) epolate 
	replace prevalence = iprevalence 
	drop iprevalence
	replace prevalence = 0.000001 if prevalence < 0

	// reshape 
	drop target_order
	rename prevalence draw_
	reshape wide draw_, i(age_group_id) j(draw_num)

//////////////////////
// format for upload
//////////////////////

/* Create a draws file with the same draws (28 day prevalence)
 across all ages.*/
preserve
	keep if age_group_id == 99
	expand 25
	replace age_group_id = _n+1
	drop model_version_id age

	// save
	order age_group_id sex_id year_id location_id measure_id modelable_entity_id
	export delimited "`data_dir'/draws/5_`location_id'_`year_id'_`sex_id'.csv", replace	

restore

preserve
	keep if age_group_id == 164
	expand 25
	replace age_group_id = _n+1
	drop model_version_id age

	// save
	order age_group_id sex_id year_id location_id measure_id modelable_entity_id
	export delimited "`data_dir'/draws/birth/5_`location_id'_`year_id'_`sex_id'.csv", replace

restore

preserve
	keep if age_group_id == 2
	expand 25
	replace age_group_id = _n+1
	drop model_version_id age

	// save
	order age_group_id sex_id year_id location_id measure_id modelable_entity_id
	export delimited "`data_dir'/draws/0-6/5_`location_id'_`year_id'_`sex_id'.csv", replace
	
restore

preserve
	keep if age_group_id == 3
	expand 25
	replace age_group_id = _n+1
	drop model_version_id age

	// save
	order age_group_id sex_id year_id location_id measure_id modelable_entity_id
	export delimited "`data_dir'/draws/7-27/5_`location_id'_`year_id'_`sex_id'.csv", replace

restore
	
