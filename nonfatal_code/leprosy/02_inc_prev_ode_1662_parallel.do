// **********************************************************************
// Purpose:        This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:         USERNAME

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}
	
	// Define arguments
	if "`1'" != "" {
		local location_id `1'
		local min_year `2'
		local tmp_dir `3'	
	}

  // write log if running in parallel and log is not already open
  cap log using "`tmp_dir'/02_temp/02_logs/prev_inc_`location_id'.smcl", replace
  if !_rc local close 1
  else local close 0

  local sex_ids 1 2
  numlist "1987/2016"
  local year_ids "`r(numlist)'"

// *********************************************************************************************************************************************************************
  // set adopath
  adopath + "FILEPATH"

// *********************************************************************************************************************************************************************

  use "`tmp_dir'/loc_iso3.dta", clear
  keep if location_id == `location_id'
  levelsof iso3, local(iso) c
  local iso "`iso'"

  quietly insheet using "`tmp_dir'/draws/cases/age_pattern_interpolated/interpolated/`location_id'.csv", clear double
  drop if year_id < 1987
  quietly drop if age_group_id == 22 | age_group_id == 27

  forvalues i = 0/999 {
    quietly replace draw_`i' = 0 if age_group_id < 4
  }
  quietly merge m:1 location_id year_id using "`tmp_dir'/data_filled.dta", keepusing(mean total_pop) keep(match) nogen
  quietly merge 1:1 location_id year_id age_group_id sex_id using "`tmp_dir'/pops.dta", keep(match) nogen
    

    generate double cases = mean * total_pop
    fastrowmean draw_*, mean_var_name(mu_cases_draw)
    quietly replace mu_cases_draw = mu_cases_draw * mean_pop
    bysort year: egen double total_cases_draw = total(mu_cases_draw)
    generate double scale = cases / total_cases_draw
    
    forvalues i = 0/999 {
      quietly replace draw_`i' = draw_`i' * scale
    }
    
    drop cases mu_cases_draw total_cases_draw scale mean total_pop mean_pop
    
	quietly merge m:1 age_group_id using "`tmp_dir'/age_map.dta", keep(3) nogen
	tempfile bs
	save `bs', replace

  ** INCIDENCE
  foreach year of local year_ids {
    foreach sex_id of local sex_ids {
      preserve
      keep if year_id == `year'
      keep if sex_id == `sex_id'
      if "`sex_id'" == "1" {
        local sex "male"
      }
      else if "`sex_id'" == "2" {
        local sex "female"
      }
      tempfile file
      save `file', replace
      keep if age < 0.5 & age > 0
      tempfile young
      save `young', replace
      use `file', clear
      drop if age < 0.5 & age > 0
      append using `young'
      keep age draw_*
      format %16.0g draw_* age
      export delimited "`tmp_dir'/draws/cases/inc_annual/incidence_`iso'_`year'_`sex'.csv", replace
      restore
    }
  }


  ** PREVALENCE
  use `bs', clear
  keep if year_id == 1987
  foreach sex_id of local sex_ids {
      preserve
      keep if sex_id == `sex_id'
      if "`sex_id'" == "1" {
        local sex "male"
      }
      else if "`sex_id'" == "2" {
        local sex "female"
      }

    forvalues x = 0/999 {
      ** Prevalence at start of each age category
        quietly generate double prev_start_`x' = 0 if age == 0 
        quietly replace prev_start_`x' = prev_start_`x'[_n-1] + (1 - prev_start_`x'[_n-1]) * (1 - exp(-(age - age[_n-1]) * draw_`x'[_n-1])) if age > 0
        
      ** Prevalence halfway each age category (half-year correction)
        quietly replace prev_start_`x' = prev_start_`x' + (1 - prev_start_`x') * (1 - exp(-(age[_n+1] - age)/2 * draw_`x')) if age < 95
        quietly replace prev_start_`x' = prev_start_`x' + (1 - prev_start_`x') * (1 - exp(-(100 - age)/2 * draw_`x')) if age == 95
        quietly drop draw_`x'
        quietly rename prev_start_`x' draw_`x'
    }
      tempfile file
      save `file', replace
      keep if age < 0.5 & age > 0
      tempfile young
      save `young', replace
      use `file', clear
      drop if age < 0.5 & age > 0
      append using `young'
      keep age draw_*
      format %16.0g draw_* age
      export delimited "`tmp_dir'/draws/cases/prev_initial/prevalence_`iso'_1987_`sex'.csv", replace
      restore
  }


// *********************************************************************************************************************************************************************

** write check here
  file open finished using "`tmp_dir'/02_temp/01_code/checks/finished_loc`location_id'.txt", replace write
  file close finished

** close logs
  if `close' log close
  clear
