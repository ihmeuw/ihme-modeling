// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:		USERNAME
// Description:	Correct dismod output for pre-control prevalence of infection and morbidity for the effect of mass treatment, and scale
// 				to the national level (dismod model is at level of population at risk).

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

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
		local tmp_in_dir `2' 
		local out_dir `3'
		local out_dir_infection `4' 
		local out_dir_lymphedema `5' 
		local out_dir_hydrocele `6'
    local in_dir `7'
    local date `8'
	}

  // write log if running in parallel and log is not already open
  cap log using "FILEPATH/`location_id'.smcl", replace
  if !_rc local close_log 1
  else local close_log 0




// *********************************************************************************************************************************************************************
  ** set central functions
  adopath + "FILEPATH"
	
// *********************************************************************************************************************************************************************
    
// By locationpull draw files and correct prevalence of infection and morbidity for effect of mass treatment and scale to national level
  // Get demographics
	get_demographics , gbd_team(epi) clear
  local year_ids `r(year_ids)'
  local sex_ids `r(sex_ids)'
	
  // Mf prevalence
	use "`tmp_in_dir'/loc_met.dta" if location_id == `location_id', clear
	levelsof ihme_loc_id, local(iso3) c

  display "`iso3' hydrocele prevalence" 
      
  ** Pull PAR adjusted mf prevalence draws 
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(10519) measure_ids(5) location_ids(`location_id') year_ids(`year_ids') sex_ids(`sex_ids') source(epi) clear
      
  keep *_id draw_*    
  quietly drop if age_group_id == 22 | age_group_id == 27
  tempfile draws
  save `draws', replace

  ** Calculate crude prevalence of mf in the population (post-PAR adjustment) -- this is in people infected space
  ** Replace with 0s for females (can't get hydrocele) or children under 5
  ** crude prevalence
  quietly merge 1:1 location_id age_group_id year_id sex_id using "`tmp_in_dir'/pops.dta", keepusing(pop_scaled) keep(master match) nogen
  forvalues i = 0/999 {
    qui replace draw_`i' = draw_`i' * pop_scaled
    qui replace draw_`i' = 0 if age_group_id < 5
    qui replace draw_`i' = 0 if sex_id == 2
  }
  qui replace pop_scaled = 0 if age_group_id < 5 | age_group_id == 164

  ** collapse by year and get population for each year
  qui fastcollapse pop_scaled draw_*, type(sum) by(year_id)
  foreach year of local year_ids {
    levelsof pop_scaled if year_id == `year', local(pop_`year') c
  }
    
  drop year_id pop_scaled
  xpose, clear promote
  format %16.0g *
  rename v1 cases_1990
  rename v2 cases_1995
  rename v3 cases_2000
  rename v4 cases_2005
  rename v5 cases_2010
  rename v6 cases_2016

  ** generate prevalence
  foreach year of local year_ids {
    gen pop_`year' = `pop_`year''
    replace cases_`year' = cases_`year' / pop_`year'
  }
  gen index = _n
  drop pop_*

  qui merge 1:1 index using "`tmp_in_dir'/hyd_regression.dta", nogen

  foreach year of local year_ids {
    qui replace cases_`year' = (a + b * cases_`year' ^ c) / (1 + b * cases_`year' ^ c)
    rename cases_`year' prev_envelope_`year'
  }
  
  drop a b c
  tempfile hyd_envelope_draws
  save `hyd_envelope_draws', replace

  ** squeeze dismod age pattern draws into envelope draws
  get_draws, gbd_id_field(modelable_entity_id) gbd_id(10994) measure_ids(5) location_ids(`location_id') year_ids(`year_ids') sex_ids(`sex_ids') source(epi) clear
  ** clean up
  keep *_id draw_*
  quietly drop if age_group_id == 22 | age_group_id == 27 | age_group_id == 33
  ** merge in population data
  quietly merge 1:1 location_id age_group_id year_id sex_id using "`tmp_in_dir'/pops.dta", keepusing(pop_scaled) keep(1 3) nogen
  preserve
  import delimited "`in_dir'/prop_pop_at_risk.csv", clear
  tempfile par
  save `par', replace
  restore
  merge m:1 year_id location_id using `par', nogen keep(1 3) keepusing(prop_at_risk)
  replace prop_at_risk = 0 if prop_at_risk == .
  ** format to merge with hydrocele envelope draws

  drop if age_group_id == 33
  foreach year of local year_ids {
    preserve
    keep if year_id == `year'
    qui set obs 1000
    gen int index = _n
    merge 1:1 index using `hyd_envelope_draws', nogen keepusing(prev_envelope_`year')
    forvalues i = 0/999 {
      ** Calculate unscaled number of hydrocele cases by age, according to dismod (assume zero hydrocele under age 5)
        replace draw_`i' = draw_`i' * pop_scaled
        replace draw_`i' = 0 if age_group_id < 6  | age_group_id == 164
        replace draw_`i' = 0 if sex_id == 2
        
      ** Create r(sum) containing total unscaled cases, according to dismod
        qui summarize draw_`i', detail
      
      ** Calculate prevalence scaled to population at risk, using scaling factor [envelope draw ] / [unscaled dismod cases draw]
        replace draw_`i' = (draw_`i' / pop_scaled) * (prev_envelope_`year'[`i'+1] * `pop_`year'') / r(sum)
        replace draw_`i' = 1 if draw_`i' > 1
        replace draw_`i' = 0 if age_group_id == 164
    }
    drop if year_id == .
    tempfile temp_`year'
    save `temp_`year''
    restore
  }
  clear
  ** This file contains hydrocele prevalence draws scaled to the hydrocele envelope at the level
  ** of population at risk.
  ** pull hydrocele 
  foreach year in 2005 2010 2016 {
    use `temp_2000'
    drop prop_at_risk
    replace year_id = `year'
    merge m:1 location_id year_id using `par', nogen keep(1 3) keepusing(prop_at_risk)
    replace prop_at_risk = 0 if prop_at_risk == .
    merge m:1 year_id location_id using "`tmp_in_dir'/coverage.dta", nogen keep(1 3) keepusing(effect_hyd*)
    forvalues i = 0/999 {
      qui replace draw_`i' = draw_`i' * effect_hyd_`i'
    }
    save `temp_`year'', replace
  }
  clear
  foreach year of local year_ids {
    append using `temp_`year''
  }
  forvalues i = 0/999 {
    qui replace draw_`i' = draw_`i' * prop_at_risk
    qui replace draw_`i' = 0 if age_group_id < 5
    qui replace draw_`i' = 0 if sex == 2
  }

  drop if missing(age_group_id) |  age_group_id == 27 |  age_group_id == 22 | age_group_id == 33

  ** Temporarily store draw file if next iteration of this loop is going to be a post-control year.

  keep location_id year_id age_group_id sex_id measure_id draw*

  export delimited using "`out_dir_hydrocele'/`location_id'.csv", replace


**********************************************************************************************
  ** Prevalence of lymphedema
**********************************************************************************************
  display "`iso3' prevalence of lymphedema"
      
  // If this is a pre-control year (<2000, or <1995 for China), predict pre-control lymphedema prevalence from mf prevalence
  local china = 0
  if regexm("`iso3'", "CHN") {
    local china = 1
  }

  use `draws', clear
  drop if age_group_id == 33

  merge 1:1 location_id year_id age_group_id sex_id using "`tmp_in_dir'/pops.dta", keepusing(pop_scaled) keep(1 3) nogen
  forvalues i = 0/999 {
    qui replace draw_`i' = draw_`i' * pop_scaled
    qui replace draw_`i' = 0 if age_group_id < 5
  }

  qui fastcollapse pop_scaled draw_*, type(sum) by(year_id)
  foreach year of local year_ids {
    levelsof pop_scaled if year_id == `year', local(pop_`year') c
  }
  drop year_id pop_scaled

  xpose, clear promote
  format %16.0g *
  rename v1 cases_1990
  rename v2 cases_1995
  rename v3 cases_2000
  rename v4 cases_2005
  rename v5 cases_2010
  rename v6 cases_2016

  ** generate prevalence
  foreach year of local year_ids {
    gen pop_`year' = `pop_`year''
    replace cases_`year' = cases_`year' / pop_`year'
  }
  gen index = _n
  drop pop_*

  merge 1:1 index using "`tmp_in_dir'/oed_regression.dta", nogen

  foreach year of local year_ids {
    replace cases_`year' = (a + b * cases_`year' ^ c) / (1 + b * cases_`year' ^ c)
    rename cases_`year' prev_envelope_`year'
  }
  keep index prev_envelope*

  tempfile oed_envelope_draws
  save `oed_envelope_draws', replace

  ** squeeze dismod age pattern draws into envelope draws
  get_draws, gbd_id_field(modelable_entity_id) gbd_id(10993) measure_ids(5) location_ids(`location_id') year_ids(`year_id') sex_ids(`sex_id') source(epi) clear
  ** clean up
  keep *_id draw_*
  quietly drop if age_group_id == 22 | age_group_id == 27 | age_group_id == 33
  merge 1:1 location_id age_group_id year_id sex_id using "`tmp_in_dir'/pops.dta", keepusing(pop_scaled) keep(1 3) nogen
  merge m:1 year_id location_id using `par', keepusing(prop_at_risk) keep(1 3) nogen

  drop if age_group_id == 33
  foreach year of local year_ids {
    preserve
    keep if year_id == `year'
    qui set obs 1000
    gen int index = _n
    merge 1:1 index using `oed_envelope_draws', nogen keepusing(prev_envelope_`year')
    forvalues i = 0/999 {
      ** Calculate unscaled number of hydrocele cases by age, according to dismod (assume zero hydrocele under age 5)
        replace draw_`i' = draw_`i' * pop_scaled
        replace draw_`i' = 0 if age_group_id < 6  | age_group_id == 164
        
      ** Create r(sum) containing total unscaled cases, according to dismod
        qui summarize draw_`i', detail
      
      ** Calculate prevalence scaled to population at risk, using scaling factor [envelope draw ] / [unscaled dismod cases draw]
        replace draw_`i' = (draw_`i' / pop_scaled) * (prev_envelope_`year'[`i'+1] * `pop_`year'') / r(sum)
        replace draw_`i' = 1 if draw_`i' > 1
        replace draw_`i' = 0 if age_group_id == 164
    }
    drop if year_id == .
    tempfile temp_`year'
    save `temp_`year''
    restore
  }

  ** currently lymphedema prevalence draws scaled to the envelope at the level of population at risk

  if `china' == 0 {
    foreach year of local year_ids {
      if `year' < 2005 {
        use `temp_`year'', clear
      }
      if `year' > 2000 & `year' < 2016 {
        local lag = `year' - 5
        use `temp_`lag'', clear
        replace year_id = `year'
      }
      if `year' == 2016 {
        use `temp_2010', clear
        replace year_id = `year'
      }

      merge m:1 year_id location_id using "`tmp_in_dir'/coverage.dta", keepusing(cov_avg5) keep(1 3) nogen

      replace age_group_id = 1 if age_group_id == 164
      gsort sex_id -age_group_id
      forvalues i = 0/999 {
        bysort sex_id: replace draw_`i' = (1 - cov_avg5) * draw_`i' + cov_avg5 * draw_`i'[_n+1] if _n < _N
        bysort sex_id: replace draw_`i' = (1 - cov_avg5) * draw_`i' if _n == _N
      }
      ** fixing the renamed age group
      replace age_group_id = 164 if age_group_id == 1
      keep sex_id age_group_id location_id year_id draw*
      ** save for next iteration of coverage adjustments
      if `year' > 1995 {
        save `temp_`year'', replace
      }

      ** adjust for proportion of the population at risk
      merge m:1 year_id location_id using `par', keepusing(prop_at_risk) keep(1 3) nogen
      replace prop_at_risk = 0 if prop_at_risk == .
      forvalues i = 0/999 {
        qui replace draw_`i' = draw_`i' * prop_at_risk
        qui replace draw_`i' = 0 if age_group_id < 5
      }
      keep sex_id age_group_id location_id year_id draw*
      ** save for upload
      tempfile draw_`year'
      save `draw_`year'', replace
    }
  }

  else if `china' == 1 {
    foreach year of local year_ids {
      if `year' == 1990 {
        use `temp_`year'', clear
      }
      if `year' > 1990 & `year' < 2016 {
        local lag = `year' - 5
        use `temp_`lag'', clear
        replace year_id = `year'
      }
      if `year' == 2016 {
        use `temp_2010', clear
        replace year_id = `year'
      }

      merge m:1 year_id location_id using "`tmp_in_dir'/coverage.dta", keepusing(cov_avg5) keep(1 3) nogen
      ** no active transmission in China, so no incident lymphedema cases
      replace cov_avg5 = 1

      replace age_group_id = 1 if age_group_id == 164
      gsort sex_id -age_group_id
      forvalues i = 0/999 {
        bysort sex_id: replace draw_`i' = (1 - cov_avg5) * draw_`i' + cov_avg5 * draw_`i'[_n+1] if _n < _N
        bysort sex_id: replace draw_`i' = (1 - cov_avg5) * draw_`i' if _n == _N
      }
      ** fixing the renamed age group
      replace age_group_id = 164 if age_group_id == 1
      keep sex_id age_group_id location_id year_id draw*
      ** save for next iteration of coverage adjustments
      save `temp_`year'', replace

      ** only want to use the 1990 PAR for china
      replace year_id = 1990
      ** adjust for proportion of the population at risk
      merge m:1 year_id location_id using `par', keepusing(prop_at_risk) keep(1 3) nogen
      replace prop_at_risk = 0 if prop_at_risk == .
      forvalues i = 0/999 {
        qui replace draw_`i' = draw_`i' * prop_at_risk
        qui replace draw_`i' = 0 if age_group_id < 5
      }
      keep sex_id age_group_id location_id year_id draw*
      replace year_id = `year'
      ** save for upload
      tempfile draw_`year'
      save `draw_`year'', replace
    }
  }

  clear
  foreach year of local year_ids {
    append using `draw_`year''
  }

  gen measure_id = 5
  keep age_group_id sex_id year_id location_id measure_id draw_*
	
  export delimited "`out_dir_lymphedema'/`location_id'.csv", replace


// *********************************************************************************************************************************************************************
