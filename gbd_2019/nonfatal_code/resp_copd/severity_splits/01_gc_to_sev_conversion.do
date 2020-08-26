//USER
//USER
//USER
//Squeeze US 2005 dismod gold class results to 1. Convert to severity splits. Save files to split the rest of the data


disp("Starting GC to SEV conversion --------------------------------------")
run "FILEPATH/get_draws.ado"
//set stata settings
	clear
	set more off
	set maxvar 32000

	// Define J drive (data) for cluster (UNIX) and Windows (Windows)
	if c(os) == "Unix" {
		local prefix "FILEPATH"
		set odbcmgr unixodbc
		local ver 1
	}
	else if c(os) == "Windows" {
		local prefix "FILEPATH"
		local ver 0
	}

	//change this if shared functions move to a new file location
	adopath + `prefix'FILEPATH

	//pass arguments
	args ver_desc
	di in red "`ver_desc'"

	//set locals
	local g1 3062 //mild: gold class 1
	local g2 3063 //moderate: gold class 2
	local g3 3064 //severe: gold class 3-4
	local copd 24543 // 10/05/2019 previous MEID 1872
	local gc `g1' `g2' `g3'

	//Load MEPS severity splits and process -- obtained MEPS draws from Carrie Purcell
	//use "FILEPATH", clear
	disp("Trying to import COPD MEPS Distributions from directory --------------------------------")
	import delimited using "FILEPATH/copd_meps_draws_2019.csv", clear
	disp("Successfully imported MEPS draws ----------------------------")
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

	disp("Gold class values from 2005 ----------------------------------")
	//load the gold class values for US 2005
	di "Get draws defaults to best model: make sure gold class models are marked best"
	//if not run on the cluster, need to have usa_goldclass_2005.dta saved already
	if c(os) == "Unix" {
		clear
		tempfile draws
		save `draws', emptyok replace
		foreach ggg of local gc{
			di `ggg'
			get_draws, gbd_id_type(modelable_entity_id) gbd_id(`ggg') location_id(102) measure_id(18) year_id(2005) sex_id(1 2) source(epi) gbd_round_id(6) decomp_step('step4') clear
			count
			if `r(N)' ==0 {
				di in red "get draws failed."
			}
			append using `draws'
			save `draws', replace
		}
	}
	else {
		di in red "Note: this will not work if usa_goldclass_2005.dta is not saved here:  FILEPATH/usa_copd_2005.dta already"
		use "FILEPATH/usa_goldclass_2005.dta", clear
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

	disp("Line 93 -----------------------------------------------------------")
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

disp("Saving -------------------------------------------")
// save
	save "FILEPATH/severity_conversions_both.dta", replace

// load COPD values
get_draws, gbd_id_type(modelable_entity_id) gbd_id(`copd') source(epi) location_id(102) year_id(2005) sex_id(1 2) gbd_round_id(6) decomp_step('step4') clear
save "FILEPATH/usa_copd_2005.dta", replace
