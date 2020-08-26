//Purpose: Prepare and save draws and mean estimates for hygiene Relative Risks 

**Housekeeping
clear all
set more off
set maxvar 30000
set obs 2

	**Set relevant locals
	local out_dir			"FILEPATH"
	local rf_new			"wash_hygiene"
	local output_version	2
	
	**save RRs (lri)
	local rr_handwashing 0.757847856806294
	local upper_handwashing 0.88301505246394
	local lower_handwashing 0.650423084480032

	**Prep data
	gen cause_id = 322 // lri
	gen location_id = 1

	gen age_group_id = 22 
	
	gen sex_id = 3
	
	gen mortality = 1
	gen morbidity = 1
	
	gen id = _n 
	gen parameter = "cat1" if id==1
	replace parameter = "cat2" if id==2 

	gen rr_mean = 1/`rr_handwashing' if id == 1
	gen rr_lower = 1/`upper_handwashing' if id == 1
	gen rr_upper = 1/`lower_handwashing' if id == 1
	
	replace rr_mean = 1 if id == 2 
	replace rr_lower = 1 if id == 2 
	replace rr_upper = 1 if id == 2 
	drop id
	
	** Create two categories and 1000 draws
	gen sd = ((ln(rr_upper)) - (ln(rr_lower))) / (2*invnormal(.975))
	forvalues draw = 0/999 {
		gen rr_`draw' = exp(rnormal(ln(rr_mean), sd))
	}	
	
	tempfile lri
	save `lri', replace

	clear
	set obs 2
	**save RRs (diarrhea)
	local rr_handwashing 0.656812252689578
	local upper_handwashing 0.713418417818743
	local lower_handwashing 0.604697502206572

	**Prep data
	gen cause_id = 302 // diarrhea
	gen location_id = 1

	gen age_group_id = 22 
	
	gen sex_id = 3
	
	gen mortality = 1
	gen morbidity = 1
	
	gen id = _n 
	gen parameter = "cat1" if id==1
	replace parameter = "cat2" if id==2 

	gen rr_mean = 1/`rr_handwashing' if id == 1
	gen rr_lower = 1/`upper_handwashing' if id == 1
	gen rr_upper = 1/`lower_handwashing' if id == 1
	
	replace rr_mean = 1 if id == 2 
	replace rr_lower = 1 if id == 2 
	replace rr_upper = 1 if id == 2 
	drop id

	** Create two categories and 1000 draws
	gen sd = ((ln(rr_upper)) - (ln(rr_lower))) / (2*invnormal(.975))
	forvalues draw = 0/999 {
		gen rr_`draw' = exp(rnormal(ln(rr_mean), sd))
	}	

	append using `lri'

// Work around to duplicate out for each age_group_id
	local ages "2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235"
	local age_app "3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235"

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


	***************************************
	** Save mean/lower/upper on /share**
	***************************************
	foreach sex in 1 2 {
		replace sex_id = `sex'
		export delimited "`out_dir'/rr_1_`sex'.csv", replace
	}
	
	// Run save_results
	clear
	quietly run "FILEPATH"
	save_results_risk, modelable_entity_id(9081) input_file_pattern("rr_{location_id}_{sex_id}.csv") description(DESCRIPTION) input_dir(`out_dir') mark_best(T) risk_type("rr") decomp_step("step3")

*******************************
**********end of code***********
*******************************
