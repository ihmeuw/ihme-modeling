//Squeeze US 2005 dismod gold class results to 1. Convert to severity splits. Save files to split the rest of the data

//set stata settings
	clear
	set more off
	set maxvar 32000
	
	//pass arguments
	args ver_desc
	di in red "`ver_desc'"

	//set locals
	local g1 3062 //mild: gold class 1
	local g2 3063 //moderate: gold class 2
	local g3 3064 //severe: gold class 3-4
	local copd 1872
	local gc `g1' `g2' `g3'

	//Load MEPS severity splits and process
	use "FILEPATH/meps_resp_copd_1000_draws.dta", clear
	gen use_me = 1
	keep use_me severity dist*
	drop dist_mean dist_lci dist_uci
	duplicates drop
	forvalues i=0/999 {
		rename dist`i' dist`i'_
	}
	reshape wide dist*_, i(use_me) j(severity) 

	tempfile sever
	save `sever', replace
	
	//load the gold class values for US 2005
	di "Get draws dUSERts to best model: make sure gold class models are marked best"

	//load in get_draws function
	do "FILEPATH/get_draws.ado"

	if c(os) == "Unix" {
		clear
		tempfile draws
		save `draws', emptyok replace
		foreach ggg of local gc{
			di `ggg'
			get_draws, gbd_id_field(modelable_entity_id) gbd_id(`ggg') location_ids(102) measure_ids(18) year_ids(2005) sex_ids(1 2) source(epi) clear
			count
			if `r(N)' ==0 {
				di in red "get draws failed."
			}
			append using `draws'
			save `draws', replace
		}
	}
	else {
		use "PATH/usa_goldclass_2005.dta", clear
	}

//Scale the draws to 1
	forvalues i = 0(1)999{
		di in red "scaling draw `i'"
		bysort location_id sex_id year_id age_group_id measure_id: egen total_draw = total(draw_`i')
		replace draw_`i' = draw_`i' / total_draw
		drop total_draw
	}
	
	gen use_me = 1
	drop model_version_id
	
	rename draw_* draw_*_
	reshape wide draw_*_, i(sex_id year_id age_group_id measure_id location_id) j(modelable_entity_id)
	
//now that we have draws scaled to 1, bring in the MEPS results
	merge m:1 use_me using `sever', assert(3) nogen

//convert from gold to meps
	forvalues j = 0(1)999 {
		di in red "Converting Draw `j'"
		//logic carried over from 2013
		gen x_asymp`j' = dist`j'_0 / draw_`j'_`g1'
		gen x_mild`j' = (dist`j'_1/(dist`j'_1+dist`j'_2))
		gen x_mod`j' = (dist`j'_2/(dist`j'_1+dist`j'_2))
		gen x_sev`j' = dist`j'_3 / draw_`j'_`g3'
		
		//clean up file
		drop draw_`j'_`g1' draw_`j'_`g2' draw_`j'_`g3' dist`j'_*
	}

// save
	save "FILEPATH/severity_conversions_both.dta", replace

	
// load COPD values
get_draws, gbd_id_field(modelable_entity_id) gbd_id(`copd') source(epi) location_ids(102) year_ids(2005) sex_ids(1 2) clear
save "FILEPATH/usa_copd_2005.dta", replace
	
	
	
	
	
	
	
