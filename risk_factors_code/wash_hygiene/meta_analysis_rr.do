**Set directories
	if c(os) == "Windows" {
		global j "FILEPATH"
		set mem 1g
	}
	if c(os) == "Unix" {
		global j "FILEPATH"
		set mem 8g
	} 

**Housekeeping
clear all
set more off
set maxvar 30000
set obs 2

	**Set relevant locals
	local out_dir			""
	local rf_new			"wash_hygiene"
	local output_version	2
	
	**save RRs from the study (RUSER and Curtis 2006) 
	local rr_handwashing 0.84
	local upper_handwashing 0.89
	local lower_handwashing 0.79

	**Prep data
	gen cause_id = 322
	gen location_id = 1
	
	gen year_id = 1990

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
	**save RRs from the study (Cairncross et al., 2010)- ****Changed from (Freeman et al 2014) for GBD 2016 due to definition change of handwashing**** 
	local rr_handwashing 0.53
	local upper_handwashing 0.76
	local lower_handwashing 0.37

	**Prep data
	gen cause_id = 302 // diarrhea
	gen location_id = 1
	
	gen year_id = 1990

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

	***************************************
	** Save mean/lower/upper on the drive**
	***************************************
	foreach sex in 1 2 {
		replace sex_id = `sex'
		export delimited "`out_dir'/rr_1_1990_`sex'.csv", replace
	}
	// Run save_results
	local dat_dir ""
	quietly run ""
	save_results, modelable_entity_id(9081) description(RR update-eliminating paratyphoid and typhoid) in_dir(`dat_dir') mark_best(yes) risk_type("rr")

*******************************
**********end of code***********
*******************************
