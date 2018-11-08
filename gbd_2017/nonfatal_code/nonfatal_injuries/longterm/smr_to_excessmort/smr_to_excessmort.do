// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		Convert standardized mortality ratios to excess mortality for the DisMod ODE solver
// Author:		USERNAME

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
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
		local 4 "01e"
		local 5 SMR_to_excessmort
		local 6 "FILEPATH"
		local 7 101
		local 8 1995
		local 9 1
	}
	// base directory on FILEPATH
	local root_j_dir `1'
	// base directory on FILEPATH
	local root_tmp_dir `2'
	// timestamp of current run (i.e. 2014_01_17)
	local date `3'
	// step number of this step (i.e. 01a)
	local step_num `4'
	// name of current step (i.e. first_step_name)
	local step_name `5'
    // directory where the code lives
    local code_dir `6'
    // iso3
	local location_id `7'
	// year
	local year `8'
	// sex
	local sex `9'
	// directory for external inputs
	local in_dir "`root_j_dir'/FILEPATH"
	// directory for output on the J drive
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on clustertmp
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	// directory for standard code files
	adopath + "FILEPATH"
	
	// write log if running in parallel and log is not already open
	local log_file "`out_dir'/FILEPATH.smcl"
	log using "`log_file'", replace name(step_worker)
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// Settings
	local slots 4
	local debug 0
	
// Filepaths
	local diag_dir "`out_dir'/FILEPATH"
	local input_dir "`tmp_dir'/FILEPATH"
	local output_dir "`tmp_dir'/FILEPATH"
	local draw_dir "`output_dir'/FILEPATH"
	local summ_dir "`output_dir'/FILEPATH"
	
// Import GBD functions
	adopath + `code_dir'/ado
	run "FILEPATH.ado"

// Start timer
	start_timer, dir("`diag_dir'") name("SMR_`location_id'_`year'_`sex'") slots(`slots')

// Where is SMR from metanalysis 
local smr_data "`in_dir'/FILEPATH.csv"

// Pull conversion table for ages to age_group_ids
	insheet using "`code_dir'/FILEPATH.csv", comma names clear
		tostring age_start, force format(%12.2f) replace
		destring age_start, replace
		tempfile ages 
		save `ages', replace

// Grab populations 
	get_demographics, gbd_team(epi) clear
	global age_group_ids `r(age_group_ids)'
	global year_ids `r(year_ids)'
	global location_ids `r(location_ids)'
	global sex_ids `r(sex_ids)'

	// Pull location_id for this ihme_loc_id
	get_location_metadata, location_set_id(35) clear
	keep if location_id == `location_id'
	local ihme_loc_id = ihme_loc_id

// Bring in mortality envelope by iso3/year/age/sex - TOTAL MORTALITY - create 1000 draws from mean, upper, lower
	get_envelope, location_id(`location_id') year_id(`year') sex_id(`sex') age_group_id($age_group_ids) with_hiv(1) with_shock(0) rates(1) clear

	keep if sex_id == `sex' & year_id == `year'
	gen ihme_loc_id = "`ihme_loc_id'"
	keep ihme_loc_id location_id sex_id age_group_id year_id mean upper lower
	calc_se lower upper, newvar(se)

	// create draws
	forvalues i = 0/999 {
		gen draw_`i' = rnormal(mean, se)
	}

	levelsof ihme_loc_id, l(ihme_loc_ids)
	rename draw* env*
	tempfile totalmort
	save `totalmort', replace
	
// Bring in SMR mortality dataset - create 1000 draws
	import delimited using "`smr_data'", delim(",") varnames(1) clear asdouble
	rename age age_start 
	tostring age_start, force format(%12.2f) replace
	destring age_start, replace 

	forvalues i = 80(5)90 {
		expand 2 if age == `i', gen(d)
		replace age = `i' + 5 if d == 1
		drop d
	}

	merge m:1 age_start using `ages', assert(match) nogen

	** only generate draws once for each mean/upper/lower combo
	tempfile allncodes
	save `allncodes', replace
	keep ncode smr ll ul
	duplicates drop
	calc_se ll ul, newvar(smr_sd)
	forvalues i = 0/999 {
		gen smr_`i' = rnormal(smr, smr_sd)
	}
	merge 1:m ncode smr ll ul using `allncodes', assert(match) nogen
	keep ncode age_group_id smr_*
	save `allncodes', replace

// Loop over N-codes and generate excess mortality 
	levelsof ncode, local(ncodes) clean
	foreach ncode of local ncodes {
		use `allncodes', clear

		keep if ncode == "`ncode'"
		merge 1:m age_group_id using `totalmort', assert(match) nogen
		forvalues i = 0/999 {
			replace env_`i' = env_`i' * (smr_`i' - 1)
			rename env_`i' draw_`i'
		}
		merge m:1 age_group_id using `ages', assert(match) nogen
		drop age_group_id
		rename age_start age
		keep location_id year_id age sex_id draw_*
		order location_id year_id sex_id age, first
		tempfile emr_`ncode'
		save `emr_`ncode'', replace
	}	

	if `sex' == 1 {
		local sex_string male
	}
	if `sex' == 2 {
		local sex_string female
	}

	tempfile appended
	foreach n of local ncodes {
		cap mkdir "`draw_dir'/`n'"
		
	* Select the part of data we want to save
		use `emr_`n'', clear
		keep age draw*
		
	* Save draws
		local filename "FILEPATH.csv"
		format draw_* %16.0g
		order age, first
		sort age
		export delimited "`draw_dir'/`n'/`filename'", delim(",") replace

	* Append to later save appended summary stats
		gen ncode = "`n'"
		cap confirm file `appended'
		if !_rc append using `appended'
		save `appended', replace
	}
	
// Save summary files
	fastrowmean draw_*, mean_var_name("mean")
	fastpctile draw_*, pct(2.5 97.5) names(ll ul)
	drop draw_*
	sort_by_ncode ncode, other_sort(age)
	format mean ul ll %16.0g
	export delimited "`summ_dir'/`filename'", delim(",") replace


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)

	end_timer, dir("`diag_dir'") name("SMR_`location_id'_`year'_`sex'")
	
	log close step_worker
	erase "`log_file'"
	