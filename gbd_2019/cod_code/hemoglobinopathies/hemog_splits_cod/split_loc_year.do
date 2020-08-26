
	// prep stata
	clear all
	set more off
	set maxvar 32000
	if ("`c(os)'"=="Windows") {
		local j "J:"
		local h "H:"
	}
	else {
		local j "FILEPATH"
		local h "~"
		set odbcmgr unixodbc
	}
	sysdir set PLUS "FILEPATH"
	
	// load functions
	run "FILEPATH"
	
	// parse incoming syntax elements

	cap program drop parse_syntax
	program define parse_syntax
		syntax, root_dir(string) parent_cause_id(string) location_id(string)
		c_local root_dir = "`root_dir'"
		c_local parent_cause_id = "`parent_cause_id'"
		c_local location_id = "`location_id'"
	end
	parse_syntax, `0'

	// import the inputs map
	import delimited using "FILEPATH", clear
	tempfile map
	save `map', replace
	levelsof modelable_entity_id if !mi(modelable_entity_id), local(dismods) clean 
	
	// interpolate dismod for all years 
	cap mkdir "FILEPATH"
        if _rc {
                !rm -rf "FILEPATH"
        	mkdir "FILEPATH"
	}

	
	foreach me of local dismods {
		interpolate, gbd_id_type(modelable_entity_id) gbd_id(`me') source(epi) age_group_id(2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235) measure_id(15) location_id(`location_id') reporting_year_start(1990) reporting_year_end(2017) gbd_round_id(7) status("best") decomp_step("step2") clear
		outsheet using "FILEPATH", comma replace
	}
	
	
	// compile interpolated
	clear
	tempfile appended
	save `appended', replace emptyok
	foreach me of local dismods {
		import delimited using "FILEPATH", clear
		append using `appended'
		save `appended', replace
	}
	!rm -rf "FILEPATH"
	
	// generate proportions
	merge m:1 modelable_entity_id using `map', nogen keep(3)
	collapse (sum) draw* , by(age_group_id sex_id year_id cause_id) 
	append using "FILEPATH"
	forvalues i = 0/999 {
		bysort age_group_id sex_id year_id : egen sc_draw_`i' = pc(draw_`i'), prop
	}
	drop sc_draw*
	levelsof cause_id, local(cause_ids) clean
	foreach cause of local cause_ids{
		preserve
			keep if cause_id == `cause'
			export delimited using "FILEPATH"
		restore
	}
	save `appended', replace
	
	// get the model draws
		get_draws, gbd_id_type(cause_id) gbd_id(`parent_cause_id') location_id(`location_id') year_id(`year_id') age_group_id(2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235) source(codem) status("best") gbd_round_id(7) decomp_step("step2") clear


	drop cause_id

	// merge on the proportions 
	joinby year_id age_group_id sex_id using "`appended'"

	// apply proportions
	 drop sc_*

	// save to folders
	levelsof cause_id, local(cause_ids) clean
	forvalues year_id = 1990/2017 {
		foreach cause of local cause_ids {
			foreach sex in 1 2 {
				preserve
					keep if cause_id == `cause' & sex_id == `sex' & year_id == `year_id'
					keep draw* age_group_id
					export delimited using "FILEPATH", replace
				restore
			}
		}
	}
