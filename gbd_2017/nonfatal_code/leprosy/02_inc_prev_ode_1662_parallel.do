// **********************************************************************
// Purpose:        This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Description:   Calculate incidence of leprosy by country-year-age-sex, using cases reported to WHO and
//                      age-patterns from dismod. Produce incidence for every year in 1890-2015, and sweep forward
//                      with ODE to arrive at prevalence predictions.
// FILEPATH/02_inc_prev_ode_1662_parallel.do

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
	else if "`1'" == "" {
		local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
		local date = subinstr("`date'"," ","_",.)
		local location_id 43911
		local min_year 1987
		local tmp_dir "FILEPATH"
	}

  // write log if running in parallel and log is not already open
  cap log using "FILEPATH/prev_inc_`location_id'.smcl", replace
  if !_rc local close 1
  else local close 0

  local sex_ids 1 2
  numlist "1987/2017"
  local year_ids "`r(numlist)'"

// *********************************************************************************************************************************************************************
  // set adopath
  adopath + "FILEPATH"

// *********************************************************************************************************************************************************************
// Merge incidence pattern with annually reported total incidence and population envelope and calculate year-specific incidences by scaling
// by [absolute reported numbers per year] / [sum of mean of all draws by country-year]. Save files for 1987-2016

  use "`tmp_dir'/loc_iso3.dta", clear
  keep if location_id == `location_id'
  levelsof iso3, local(iso) c
  local iso "`iso'"

  quietly insheet using "FILEPATH/`location_id'.csv", clear double
  drop if year_id < 1987
  quietly drop if age_group_id == 22 | age_group_id == 27

** changed to age_group 4 as per last year results
  forvalues i = 0/999 {
    quietly replace draw_`i' = 0 if age_group_id < 4
  }
  quietly merge m:1 location_id year_id using "`tmp_dir'/data_filled.dta", keepusing(mean total_pop) keep(match) nogen
  quietly merge 1:1 location_id year_id age_group_id sex_id using "`tmp_dir'/pops.dta", keep(match) nogen

  // Generate scaling factor [absolute reported numbers per year] / [sum of mean of all draws by country-year] and
  // scale dismod incidences to have mean equal to the annually reported number of cases.
  // NB.: total cases for subnational geographies are assumed to be national reported incidence * population in subnational area.
    generate double cases = mean * total_pop
    fastrowmean draw_*, mean_var_name(mu_cases_draw)
    quietly replace mu_cases_draw = mu_cases_draw * mean_pop
    bysort year: egen double total_cases_draw = total(mu_cases_draw)
    generate double scale = cases / total_cases_draw

    forvalues i = 0/999 {
      quietly replace draw_`i' = draw_`i' * scale
    }

    drop cases mu_cases_draw total_cases_draw scale mean total_pop mean_pop

  // Write year-specific incidence files by country (using ISO-3) and sex for 1987-2015, then calculate prevalent cases that have ever had leprosy for first year of data
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
      export delimited "FILEPATH/incidence_`iso'_`year'_`sex'.csv", replace
      restore
    }
  }

  /*keep measure_id location_id year_id sex_id age_group_id draw_*
  export delimited "FILEPATH/`location_id'.csv", replace */

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
      export delimited "FILEPATH/prevalence_`iso'_1987_`sex'.csv", replace
      restore
  }


// *********************************************************************************************************************************************************************

** write check here
  file open finished using "FILEPATH/finished_loc`location_id'.txt", replace write
  file close finished

** close logs
  if `close' log close
  clear
