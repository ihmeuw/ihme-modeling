 *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This is the parallel portion of the Leprosy india adjustment code
// Author:		USERNAME

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILENAME"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILENAME"
	}

// define locals from qsub command
	local location 		`1'
	local tmp_dir		`2'

	// write log if running in parallel and log is not already open
	cap log using "`tmp_dir'/02_temp/02_logs/`location'.smcl", replace
	if !_rc local close 1
	else local close 0

	// directory for standard code file
	adopath + FILENAME

	** get location info
	get_location_metadata, location_set_id(35) clear
	keep if location_id == `location'
	levelsof parent_id, local(parent)

	*********************************************************************************************************************************************************************
	*********************************************************************************************************************************************************************
	** get population info
	numlist "2/20 30/32 235"
	local ages = "`r(numlist)'"
	get_population, location_id(`location') sex_id("1 2") age_group_id("`ages'") year_id(2010) clear
	** get the age groups
	merge m:1 age_group_id using "`tmp_dir'/age_map.dta", nogen keep(3)
	drop age_group_id
	tempfile pop
	save `pop', replace

	** get draws here
	** male
	import delimited "`tmp_dir'/draws/cases/inc_annual/incidence_IND_`location'_2010_male.csv", clear
	gen sex_id = 1
	tempfile male
	save `male', replace

	** female
	import delimited "`tmp_dir'/draws/cases/inc_annual/incidence_IND_`location'_2010_female.csv", clear
	gen sex_id = 2
	tempfile female
	save `female', replace

	append using `male'
	gen location_id = `location'
	** set up merge with population
	destring age, replace
	merge 1:1 sex_id age using `pop', nogen keep(3)
	** convert into total cases for collapse
	forvalues i = 0/999 {
		replace draw_`i' = draw_`i' * population
	}
	collapse(sum) draw_* population, by(sex_id location_id)
	** convert back into incidence
	forvalues i = 0/999 {
		replace draw_`i' = draw_`i' / population
	}
	drop population
	tempfile draws
	save `draws', replace

	** pull in india data
	import excel "FILENAME/india_phfi_splits.xlsx", firstrow clear
	keep if location_id == `location' | location_id == `parent'
	replace location_id = `location'
	replace cases = 1 if cases == 0
	forvalues i = 0/999 {
		qui gen gammaA = rgamma((cases), 1)
		qui gen gammaB = rgamma((sample_size - cases), 1)
		qui gen adj_`i' = (gammaA / (gammaA + gammaB))
		replace adj_`i' = 0 if adj_`i' == .
		drop gamma*
	}
	keep location_id adj_*
	merge 1:m location_id using `draws', nogen keep(3)
	** calculate adjustment
	forvalues i = 0/999 {
		replace adj_`i' = adj_`i' / draw_`i'
	}
	drop draw_*
	tempfile adjustment
	save `adjustment', replace
	forvalues year = 1987/2016 {
		** male
		import delimited "`tmp_dir'/draws/cases/inc_annual/incidence_IND_`location'_`year'_male.csv", clear
		gen sex_id = 1
		gen location_id = `location'
		merge m:1 location_id sex_id using `adjustment', nogen keep(3)
		forvalues i = 0/999 {
			replace draw_`i' = draw_`i' * adj_`i'
		}
		keep draw_* age
		tostring age, replace force
		destring age, replace force
		replace age = round(age, 0.01)
		export delimited "`tmp_dir'/draws/cases/inc_annual/incidence_IND_`location'_`year'_male.csv", replace
		** now female
		import delimited "`tmp_dir'/draws/cases/inc_annual/incidence_IND_`location'_`year'_female.csv", clear
		gen sex_id = 2
		gen location_id = `location'
		merge m:1 location_id sex_id using `adjustment', nogen keep(3)
		forvalues i = 0/999 {
			replace draw_`i' = draw_`i' * adj_`i'
		}
		keep draw_* age
		tostring age, replace force
		destring age, replace force
		replace age = round(age, 0.01)
		export delimited "`tmp_dir'/draws/cases/inc_annual/incidence_IND_`location'_`year'_female.csv", replace
	}

	** write check
	file open finished using "`tmp_dir'/02_temp/01_code/checks/finished_`location'.txt", replace write
	file close finished
