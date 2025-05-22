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
	
	if "`1'" == "" {
	global prefix FILEPATH
	local 1 otp
	local 2 "FILEPATH"
	local 3 "`2'/FILEPATH"
	local 4 "N30 N31 N32 N47"
	local 5 "N1	N2 N4 N5 N7 N3 N6"
	local 6 "`2'/FILEPATH"
	local 7 "FILEPATH"
	local 8 "`3'/FILEPATH.csv"
	local 9 "FILEPATH"
	local 10
	local 11
	local 12
	}

	local inp `1'
	local step_dir `2'
	local data_dir `3'
	local no_lt `4'
	local all_lt `5'
	local savedir `6'
	local code_dir `7'
	local dw_file `8'
	local gbd_ado `9'
	local diag_dir `10'
	local name `11'
	local slots `12'
	
	local log_file "`step_dir'/FILEPATH.smcl"
	log using "`log_file'", name(run_regression) replace
	
	adopath + `code_dir'/ado
	adopath + "FILEPATH"
	
	local debug 99
	set type double, perm
	
	load_params
	
	insheet using "`code_dir'/FILEPATH.csv", comma names clear
		tempfile new_ages
		save `new_ages', replace

	get_demographics_template, gbd_team(epi) clear
	rename age_group_ids age_group_id
	rename location_ids location_id
	rename sex_ids sex_id
	rename year_ids year_id
		tempfile demographics
		save `demographics', replace

	get_location_metadata, location_set_id(2) clear
		keep location_id location_name
		tempfile meta
		save `meta', replace

	use `demographics', clear
	merge m:1 location_id using `meta', keep(3) nogen

	merge m:1 age_group_id using `new_ages', keep(3) nogen
		keep if location_name == "Qinghai" | location_name == "Washington" | location_name == "Netherlands"
		gen iso3 = "CHN" if location_name == "Qinghai"
		replace iso3 = "USA" if location_name == "Washington"
		replace iso3 = "NLD" if location_name == "Netherlands"
	keep if year == 2000
	keep iso3 sex age_start
	rename sex_id sex
	rename age_start age
	drop if age < 1 & age != 0
	expand 48
	bysort iso3 sex age: gen n_code_num = _n
	rename age age_gr
	gen n_code = n_code_num
	tostring n_code, replace
	replace n_code = "N"+n_code
			gen dummy_N21 = 1 if n_code_num == 21
			replace dummy_N21 = 0 if dummy_N21 == .
		gen dummy_N28 = 1 if n_code_num == 28
			replace dummy_N28 = 0 if dummy_N28 == .
		gen dummy_N41 = 1 if n_code_num == 41
			replace dummy_N41 = 0 if dummy_N41 == .
	gen nvrinj = 0
	tempfile square
	save `square', replace
	
	use FILEPATH, clear
	
	if `inp'==1 keep if inlist(inpatient,1,2) | inpatient == .
	else if `inp'==0 keep if inpatient == 0 | inpatient == .
	tempfile current_ds
	
	save `current_ds', replace
	
		use `current_ds', clear
		
		local no_lt_tot `no_lt_tot' `no_lt'
		
		foreach n of local no_lt {
			replace n_code_num = 0 if n_code == "`n'"
			replace n_code = "" if n_code == "`n'"
		}
	
		save `current_ds', replace
		
		rename never_injured nvrinj
		gen dummy_N21 = 1 if n_code_num == 21
			replace dummy_N21 = 0 if dummy_N21 == .
		gen dummy_N28 = 1 if n_code_num == 28
			replace dummy_N28 = 0 if dummy_N28 == .
		gen dummy_N41 = 1 if n_code_num == 41
			replace dummy_N41 = 0 if dummy_N41 == .

			mixed logit_dw b0.n_code_num c.age_gr b0.nvrinj b0.nvrinj#c.age_gr c.age_gr#dummy_N21 c.age_gr#dummy_N28 c.age_gr#dummy_N41 || iso3: || id:
	
	if 1==2 {
		drop if n_code_num == 0
		linear_fixed_predict_draws obs, n($drawnum)
		drop if obs_1 == .
		
		foreach var of varlist obs_* {
			replace `var' = logit_dw - `var'
			local newvar = subinstr("`var'","obs_","resid_",1)
			rename `var' `newvar'
		}
		
		replace n_code_num = 0
		linear_fixed_predict_draws cf, n($drawnum)
		
		foreach var of varlist cf_* {
			local resid_var = subinstr("`var'","cf_","resid_",1)
			replace `var' = `var' + `resid_var'
		}
		
		foreach var of varlist cf_* {
			local drawvar = subinstr("`var'","cf_","draw_",1)
			gen `drawvar' = 1 - ((1-invlogit(logit_dw))/(1-invlogit(`var')))
			drop `var'
		}

			tempfile predictions
			save `predictions', replace
			
			tostring age, replace force format(%12.3f)
			destring age, replace force
			merge m:1 iso3 age_gr sex using `totaldw'
			keep if _merge == 3
			drop _merge
			forvalues i = 0/999 {
				gen draw_`i' = 1 - ((1-invlogit(logit_dw))/(1-invlogit(total_dw)))
			}
			}
			
		use `square', clear
		predict obs_1
		replace n_code_num = 0
		replace dummy_N21 = 0
		replace dummy_N28 = 0
		replace dummy_N41 = 0
		predict cf_1
		gen draw_ = 1 - ((1-invlogit(obs_1))/(1-invlogit(cf_1)))
		
		gen n_ = 1
		collapse (mean) draw_* (sum) n_, by(age n_code)
		drop if n_code == ""
		
		egen high_prob = rowmax(draw_*)
		levelsof n_code if high_prob <= 0, l(no_lt) c
		drop high_prob
				
		
	foreach var of varlist draw_* {
		replace `var' = 0 if `var' < 0
		replace `var' = 1 if `var' > 1
	}
	
	foreach a in no_lt_tot all_lt {
		if "`a'" == "no_lt_tot" local value = 0
		else if "`a'" == "all_lt" local value = 1
		gen `a' = 0
		foreach n of local `a' {
			drop if n_code == "`n'"
			local new_obs = _N + 1
			set obs `new_obs'
			replace n_code = "`n'" in `new_obs'
			replace `a' = 1 in `new_obs'
			foreach var of varlist draw_* {
				replace `var' = `value' in `new_obs'
			}
		}
	}

	
	tempfile main
	save `main', replace
	import delimited using "`dw_file'", delim(",") clear varnames(1)
	rename draw* gbd_draw_*
	merge 1:m n_code using `main'
	keep if _m == 3 | all_lt == 1 | no_lt_tot == 1

	preserve
		egen gbd_mean = rowmean(gbd_draw_*)
		egen followup_mean = rowmean(draw_*)
		drop draw_* gbd_draw_*
	restore

	forvalues i = 0/999 {
	replace gbd_draw_`i' = draw_/gbd_draw_`i'
	replace gbd_draw_`i' = 1 if gbd_draw_`i'>1 & gbd_draw_`i' !=.
	rename gbd_draw_`i' draw_`i'
	}
	foreach var of varlist draw_* {
		replace `var' = 1 if `var' > 1 & `var' != .
	}
	
	cap mkdir "`savedir'/FILEPATH"
	save "`savedir'/FILEPATH.dta", replace

	local check_file_dir "`data_dir'/FILEPATH"
	file open done_summary using "`check_file_dir'/FILEPATH.txt", replace write
	file close done_summary
	
	
	log close run_regression
	
