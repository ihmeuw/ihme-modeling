//Purpose: Prepare and save draws and mean estimates for sanitation Relative Risks 

clear
set more off
set maxvar 30000

//set relevant locals
	local out_dir_draws		"FILEPATH"
	local rf_new			"wash_sanitation"
	
** Create RR template
	clear
	set obs 3
	
	** Risk
	gen risk = "`rf_new'"
	gen mortality = 1 
	gen morbidity = 1 
	
	**gen parameter
	gen id = _n
	tostring(id), replace
	gen parameter = "cat" + id
	drop id 
	
**Prep data
	gen cause_id = 302 //diarrhea
	gen location_id = 1

	//Sanitation Categories//
	**sewer 
	gen rr_mean = 1 if parameter == "cat3"
	gen rr_lower = 1 if parameter == "cat3"
	gen rr_upper = 1 if parameter == "cat3"
	
	**improved (other than sewer)
	replace rr_mean = 0.770266312790758/0.312443264106073 if parameter == "cat2"
	replace rr_lower = 0.808988163977198/0.344083235545108 if parameter == "cat2"
	replace rr_upper = 0.733397865431555/0.283712727621278 if parameter == "cat2"

	**unimproved 
	replace rr_mean = 1/0.312443264106073 if parameter == "cat1"
	replace rr_lower = 1/0.344083235545108 if parameter == "cat1"
	replace rr_upper = 1/0.283712727621278 if parameter == "cat1"

	** Create two categories and 1000 draws
	gen sd = ((ln(rr_upper)) - (ln(rr_lower))) / (2*invnormal(.975))
	forvalues draw = 0/999 {
		gen rr_`draw' = exp(rnormal(ln(rr_mean), sd))
	}	

	// Work around to duplicate out for each age_group_id
	local ages "2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235"
	local age_app "3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235"

	gen age_group_id = .
	foreach x of local ages {
		replace age_group_id = `x'
		tempfile temp_`x'
		save `temp_`x'', replace
	}

	use `temp_2', clear
	foreach x of local age_app {
		append using `temp_`x''
	}

	// Work around to duplicate out for each estimation year_id
	local years "1990 1995 2000 2005 2010 2015 2017 2019"
	local year_app "1995 2000 2005 2010 2015 2017 2019"
	
	gen year_id = .	
	foreach x of local years {
		replace year_id = `x'
		tempfile temp_`x'
		save `temp_`x'', replace
	}
	
	use `temp_1990', clear
	foreach x of local year_app {
		append using `temp_`x''
	}
	
	***************************
	** Save draws on clustertmp**
	***************************
	gen sex_id = .
	foreach sex in 1 2 {
		replace sex_id = `sex'
		export delimited "`out_dir_draws'/rr_1_`sex'.csv", replace
	}
	// Run save_results
	clear
	quietly run "FILEPATH"
	save_results_risk, modelable_entity_id(9018) input_file_pattern("rr_{location_id}_{sex_id}.csv") description(DESCRIPTION) input_dir(`out_dir_draws') mark_best(T) risk_type("rr") decomp_step("step3")

*******************************
**********end of code***********
*******************************
