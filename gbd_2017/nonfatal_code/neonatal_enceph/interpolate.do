 
clear all
set more off
set maxvar 30000
version 13.0
 
		if c(os) == "Windows" {
			local j FILEPATH

		}
		if c(os) == "Unix" {
			local j FILEPATH
		} 


local me_id `1'
local acause `2'
local location_id `3'
local year_id `4'
local sex_id `5'
 
adopath + FILEPATH

run FILEPATH

 
local data_dir FILEPATH
 
	if `me_id' == 1557 {
		di "Me_id is `me_id'"
		local copula_me 1557
	}
	if `me_id' == 1558 {
		di "Me_id is `me_id'"
		local copula_me 1558
	}
	if `me_id' == 1559 {
		di "Me_id is `me_id'"
		local copula_me 1559
	}

	if `me_id' == 2525 {
		di "Me_id is `me_id'"
		local copula_me 2525
	}

	if `me_id' == 1594 {
		di "Me_id is `me_id'"
		local copula_me 1594
	}



di "Location = `location_id', year = `year_id', sex = `sex_id', me_id = `me_id', copula_me = `copula_me' acause = `acause'"

get_draws, gbd_id_type(modelable_entity_id) gbd_id(`copula_me') source(epi) measure_id(5) location_id(`location_id') year_id(`year_id') age_group_id(2 3) sex_id(`sex_id') gbd_round_id(5) status(best) clear

	reshape long draw_, i(age_group_id) j(draw_num)
	rename draw_ prevalence
 
	bysort age_group_id: gen target_order = _n

 
	sort age_group_id prevalence
	bysort age_group_id: gen sort_order = _n

 
	preserve
	keep if age_group_id == 2 
	rename prevalence age_group_id_2
	tempfile template
	save `template', replace
	restore
 
	preserve
	keep if age_group_id == 3
	rename prevalence age_group_id_3
	drop target_order draw_num
	tempfile stamp
	save `stamp', replace
	restore, not
	
 
	merge 1:1 sort_order using `template'
	drop _merge age_group_id sort_order
	sort target_order

 
	reshape long age_group_id_, i(draw_num) j(age_group_id) 
	rename age_group_id_ prevalence

 
	expand 2 if age_group_id == 3, gen(expanded)
	replace age_group_id = 99 if expanded == 1
	replace prevalence = . if expanded == 1
	drop expanded
	expand 2 if age_group_id == 2, gen(expanded)
	replace age_group_id = 164 if expanded == 1
	replace prevalence = . if expanded == 1
	drop expanded
 
	gen age = 3/365 
 
	
	replace age = 0 if age_group_id == 164 
 
	
	replace age = (27-7)/(365) if age_group_id == 3 
 
	
	replace age = 28/365 if age_group_id == 99
  
 
	sort draw_num age_group_id
	ipolate prevalence age, by(draw_num) generate(iprevalence) epolate 
	replace prevalence = iprevalence 
	drop iprevalence


	drop target_order
	rename prevalence draw_
	reshape wide draw_, i(age_group_id) j(draw_num)
 
preserve
	keep if age_group_id == 99
	expand 25
	replace age_group_id = _n+1
	drop model_version_id age
 
	order age_group_id sex_id year_id location_id measure_id modelable_entity_id
	export delimited "FILEPATH/5_`location_id'_`year_id'_`sex_id'.csv", replace
 
	

restore

preserve
	keep if age_group_id == 164
	expand 25
	replace age_group_id = _n+1
	drop model_version_id age
 
	order age_group_id sex_id year_id location_id measure_id modelable_entity_id
	export delimited "FILEPATH/5_`location_id'_`year_id'_`sex_id'.csv", replace
 

restore

preserve
	keep if age_group_id == 2
	expand 25
	replace age_group_id = _n+1
	drop model_version_id age

 
	order age_group_id sex_id year_id location_id measure_id modelable_entity_id
	export delimited "FILEPATH/5_`location_id'_`year_id'_`sex_id'.csv", replace 

restore

preserve
	keep if age_group_id == 3
	expand 25
	replace age_group_id = _n+1
	drop model_version_id age

	// save
	order age_group_id sex_id year_id location_id measure_id modelable_entity_id
	export delimited "FILEPATH/5_`location_id'_`year_id'_`sex_id'.csv", replace 

restore
	
