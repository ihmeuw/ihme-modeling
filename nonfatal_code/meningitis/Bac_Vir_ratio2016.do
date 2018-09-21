// *********************************************************
// Author:	
// Date:	
// Purpose:	Calculate bacterial vs viral meningitis ratio
// *********************************************************

// prep stata
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "J:"
	}

// set locals
	// file path for inputs
	local in_dir "IN_DIR"

	// file for viral hospital data
	local viral_hosp "VIRAL_HOSPITAL_DATA"
	// file for viral marketscan data
	local viral_ms "VIRAL_MS_DATA"

	// file for bacterial hospital data
	local bacterial_hosp "BACTERIAL_HOSPITAL_DATA"
	// file for bacterial ms data
	local bacterial_ms "BACTERIAL_MS_DATA"

	// make bacterial hospital data inpatient only
	import excel "`bacterial_hosp'", firstrow clear
	collapse(mean) mean_2, by (sex age_start age_end) fast
	rename mean_2 inc_bacterial
	gen type_data = "hospital"
	preserve

	// load bacterial marketscan data
	import excel "`bacterial_ms'", firstrow clear
	collapse(sum) cases sample_size, by(sex age_start age_end) fast
	gen type_data = "ms"
	replace age_end = 99 if (age_end == 100)
	gen inc_bacterial = cases/sample_size
	drop cases sample_size
	tempfile ms_b
	save `ms_b'
	restore

	// append bacterial data
	append using `ms_b'
	collapse(mean) inc_bacterial, by (sex age_start age_end) fast
	tempfile bacterial
	save `bacterial'

	// import viral hospital data (both inpatient and outpatient)
	import excel "`viral_hosp'", firstrow clear
	collapse(mean) mean_2, by(sex age_start age_end) fast
	gen type_data = "hospital"
	rename mean_2 inc_viral
	preserve

	// load viral marketscan
	import excel "`viral_ms'", firstrow clear
	replace age_end = 99 if (age_end == 100)
	collapse(sum) cases sample_size, by(sex age_start age_end) fast
	gen inc_viral = cases / sample_size
	drop cases sample_size
	gen type_data = "ms"
	tempfile ms_v
	save `ms_v'
	restore

	// append viral data
	append using `ms_v'	
	collapse(mean) inc_viral, by (sex age_start age_end) fast
	tempfile viral
	save `viral', replace

	// merge with bacterial
	merge 1:1 sex age_start age_end using `bacterial', nogen keep(1 3)
	gen ratio = inc_viral / inc_bacterial
	//drop if age_start == 0 & age_end == 4

	foreach num of numlist 1/2 {
		count
		gen row = r(N)
		expand 2 if age_start == 0
		gene seqnum = _n
		replace age_start = .01 if row < seqnum & `num' == 1
		replace age_start = .1 if row < seqnum & `num' == 2
		drop row seqnum
	}
	sort sex age_start

	lowess ratio age_start if sex == "Male", gen(ratio_male) nograph
	lowess ratio age_start if sex == "Female", gen(ratio_female) nograph

	replace ratio = ratio_male if ratio_female == .
	replace ratio = ratio_female if ratio_male == .
	drop ratio_*

	so sex age_start
	gen age_group_id = _n + 1
	replace age_group_id = age_group_id - 23 if sex == "Male"
	replace age_group_id = 30 if (age_start == 80 & age_end == 84)
	replace age_group_id = 31 if (age_start == 85 & age_end == 89)
	replace age_group_id = 32 if (age_start == 90 & age_end == 94)
	replace age_group_id = 235 if (age_start == 95 & age_end == 99)
	gen sex_id = 2
	replace sex_id = 1 if sex == "Male"
	drop sex age_start age_end inc_*

	save "`in_dir'/bac_vir_ratio_2016.dta", replace
	
	clear
	