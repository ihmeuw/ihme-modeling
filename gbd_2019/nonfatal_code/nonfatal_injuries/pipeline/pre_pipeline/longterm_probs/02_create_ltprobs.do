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
	if "`1'"=="" {
		local 1 FILEPATH
		local 2 FILEPATH
		local 3 DATE
		local 4 "04a"
		local 5 prob_long_term
		local 6 "FILEPATH"
		local 7 89
	}
	local root_j_dir `1'
	local root_tmp_dir `2'
	local date `3'
	local step_num `4'
	local step_name `5'
    local code_dir `6'
	local location_id `7'
	local in_dir "`root_j_dir'/FILEPATH"
	local out_dir "`root_j_dir'/FILEPATH"
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	adopath + "FILEPATH"
	
	log using "`out_dir'/FILEPATH.smcl", replace name(worker)
	
	set seed 0
	local debug 99
	set type double, perm

	adopath + "`code_dir'/ado"

	local slots 4
	local diag_dir "`out_dir'/FILEPATH"
	
	local draw_dir "`tmp_dir'/FILEPATH"
	local summ_dir "`tmp_dir'/FILEPATH"
	
	load_params
	get_demographics, gbd_team(epi) clear
	local reporting_years `r(year_ids)'

	import excel "`code_dir'/FILEPATH.xlsx", clear sheet("steps") firstrow
	keep if name == "impute_short_term_shock_inc"
	local this_step_num = step[1]
	local shock_output_dir "`root_tmp_dir'/FILEPATH"
	
	get_demographics, gbd_team(cod) clear
	global years `r(year_ids)'

	use "`out_dir'/FILEPATH.dta"
	keep if location_id == `location_id'
	tempfile pct_treated
	save `pct_treated'
	
	
	import delimited using "`tmp_dir'/FILEPATH.csv", delim(",") clear
	tempfile prob_draws
	save `prob_draws', replace

	
	import excel using "`in_dir'/FILEPATH.xlsx", cellrange(A2:E49) firstrow clear
	drop Nname
	rename Ncode n_code
	drop if mean == "n/a"
	replace LL = 1 if mean == "same"
	replace UL = 1 if mean == "same"
	replace mean = "1" if mean == "same"
	destring mean, replace force
	
	calc_se LL UL, newvar(se)
	forvalues x = 0/$drawmax {
		gen mult_draw_`x' = rnormal(mean,se)
		replace mult_draw_`x' = mean if se == 0
		
	}
	drop mean se LL UL
	
	merge 1:m n_code using `prob_draws'
	keep if _merge == 3
	drop _merge
	gen location_id = `location_id'
	tempfile probs_and_mult
	save `probs_and_mult'
	
	get_demographics, gbd_team(cod) clear
	global year_ids `r(year_ids)'
	foreach year of global year_ids {
		use `probs_and_mult', clear
		tempfile `year'
		gen year_id = `year'
		merge m:1 year_id using `pct_treated', keep(match) nogen
	
		forvalues x = 0/$drawmax {
			replace mult_draw_`x' = 1 if mult_draw_`x' == . & inpatient == 0
			gen untreated_`x' = draw_`x' * mult_draw_`x'
			replace untreated_`x' = 1 if mult_draw_`x' == .
			
			replace draw_`x' = draw_`x' * pct_treated_`x' + untreated_`x' * (1 - pct_treated_`x')
			
			replace draw_`x' = 1 if draw_`x' > 1
		
			drop untreated_`x' mult_draw_`x' pct_treated_`x'
		}
		
		

		rename n_code ncode
		rename age_gr age
		keep ncode age inpatient draw*
		order ncode inpatient, first
		sort_by_ncode ncode, other_sort(inpatient)
		format draw* %16.0g
		cap mkdir "`draw_dir'/FILEPATH"
		cap mkdir "`draw_dir'/FILEPATH"
		export delimited using "`draw_dir'/FILEPATH.csv", delimiter(",") replace
		
		fastrowmean draw*, mean_var_name(mean)
		fastpctile draw*, pct(2.5 97.5) names(ll ul)
		drop draw*
		format mean ul ll %16.0g
		export delimited "`summ_dir'/FILEPATH.csv", delim(",") replace

		
	}

		
	log close worker
	erase "`out_dir'/FILEPATH.smcl"
	
