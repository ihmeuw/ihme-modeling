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

  run "FILEPATH/get_population.ado"
	
  use FILEPATH/deathAndCaseDraws.dta, clear
  drop deaths_* cdr_* total_? total_?? total_???
  capture drop *Mean
  
  merge m:1 location_id year_id using FILEPATH/speciesSplit.dta, assert(3) nogenerate

   
  local A1 = 1884  // cases with sleeping disorder (Blum et.al. 2006)*/
  local A2 = 2533 - `A1'  // cases without sleeping disorder
  forvalues i = 0/999 {
    local a1 = rgamma(`A1',1)
    local a2 = rgamma(`A2',1)
    local prop_sleep_`i' = `a1' / (`a1' + `a2')  // implies: prop_sleep ~ beta(positives,negatives)
  }
  

* Generate 1000 draws of total duration of symptoms in untreated cases (based on Checchi 2008 BMC Inf Dis)
  local mean_g = 1026 / 365  // average total duration in years
  local lower_g = 702 / 365
  local upper_g = 1602 / 365
  local sd_g = (ln(`upper_g')-ln(`lower_g'))/(invnormal(0.975)*2)
  local mu_g = ln(`mean_g') - 0.5*`sd_g'^2  // the mean of a log-normal distribution = exp(mu + 0.5*sigma^2)
  
  local mean_r = 0.5
  local sd_r = `sd_g' * `mean_r' / `mean_g'
  local mu_r = ln(`mean_r') - 0.5*`sd_r'^2  // the mean of a log-normal distribution = exp(mu + 0.5*sigma^2)

  
  forvalues x = 0/999 {
    local duration_g_`x' = exp(rnormal(`mu_g',`sd_g'))
	local duration_r_`x' = exp(rnormal(`mu_r',`sd_r'))
    }



* Add placeholder years as copies of last year to allow the following loop to estimate prevalence for the last year.
  forvalues y = 2016/2021 {
    quietly expand 2 if year_id == `y', g(new)
    quietly replace year_id = `y' + 1 if new == 1
    quietly drop new
  }

   
  sort ihme_loc_id year_id
  forvalues y = 0/999 {
  * Calculate number of prevalent cases based on reported incident cases in the current year
    foreach species in g r {
		quietly {
			if "`species'"=="g" generate prev_total_`species'_`y' = .5 * total_reported * pr_`species'
			else generate prev_total_`species'_`y' = .5 * (total_reported + undetected_`y') * pr_`species'
			
			generate prev_sleep_`species'_`y' = `prop_sleep_`y'' * prev_total_`species'_`y' * 0.5
			generate prev_disf_`species'_`y' = prev_total_`species'_`y' - prev_sleep_`species'_`y'
			}
			assert !missing(prev_total_`species'_`y')
	
		if "`species'"=="g" {
			* Number of years from which we will add all mortality cases
			local full_year = floor(`duration_g_`y'')

			* Add undetected cases from coming years, as appropriate given the duration
			forvalues n = 1/`full_year' {
				quietly {
					bysort location_id (year_id): replace prev_total_`species'_`y' = prev_total_`species'_`y' + (undetected_`y'[_n+`n'] * pr_`species') if `n' > 0
					bysort location_id (year_id): replace prev_sleep_`species'_`y' = prev_sleep_`species'_`y' + 0.5 * (undetected_`y'[_n+`n'] * pr_`species') if `n' > 0
					bysort location_id (year_id): replace prev_disf_`species'_`y' = prev_disf_`species'_`y' + 0.5 * (undetected_`y'[_n+`n'] * pr_`species') if `n' > 0
      
					if `n' == `full_year' | `n' == 0 {
						bysort location_id (year_id): replace prev_total_`species'_`y' = prev_total_`species'_`y' + (undetected_`y'[_n+`n'+1] * pr_`species') * (`duration_`species'_`y'' - `full_year')
						bysort location_id (year_id): replace prev_sleep_`species'_`y' = prev_sleep_`species'_`y' + 0.5 * (undetected_`y'[_n+`n'+1] * pr_`species') * (`duration_`species'_`y'' - `full_year')
						bysort location_id (year_id): replace prev_disf_`species'_`y' = prev_disf_`species'_`y' + 0.5 * (undetected_`y'[_n+`n'+1] * pr_`species') * (`duration_`species'_`y'' - `full_year')
						}
					}
				}
			}
		
		quietly {	
			count if missing(prev_total_`species'_`y') & year <= 2016
				if r(N) != 0 noisily di "Missing values of prev_total_`species'_`y'"
			count if missing(prev_sleep_`species'_`y') & year <= 2016
				if r(N) != 0 noisily di "Missing values of prev_sleep_`species'_`y'"
			count if missing(prev_disf_`species'_`y') & year <= 2016
				if r(N) != 0 noisily di "Missing values of prev_disf_`species'_`y'"
			}
		}	  
	di "." _continue
    }

 drop if year_id>2016
 keep location_id year_id prev_* 
 
  cross using FILEPATH/ageSexCurve.dta
  tempfile hat
  save `hat', replace
  
  foreach var in location year age_group {
	levelsof `var'_id, local (`var's) clean
	}
	
  get_population, location_id(`locations') age_group_id(`age_groups') sex_id(1 2) year_id(`years') clear
  drop process_version_map_id
  merge 1:1 location_id age_group_id sex_id year_id using `hat', assert(3) nogenerate
  

  
  forvalues i = 0/999 {
	quietly {
	generate temp1 = exp(rnormal(ageSexCurve, ageSexCurveSe)) * population
	replace  temp1 = 0 if age_group_id <= 4
	bysort location_id year_id: egen temp2 = total(temp1)

	foreach species in g r {
		replace prev_total_`species'_`i' = prev_total_`species'_`i' * (temp1 / temp2) / population
		replace prev_sleep_`species'_`i' = prev_sleep_`species'_`i' * (temp1 / temp2) / population
		replace prev_disf_`species'_`i'  = prev_disf_`species'_`i'  * (temp1 / temp2) / population
		}
		
	drop temp1 temp2
	}
	di "." _continue
	}
  
 
 rename prev_* *
 generate measure_id = 5
 save FILEPATH/prevDraws.dta, replace 
 
 use  FILEPATH/incAndDeathDraws.dta, clear
 keep location_id age_group_id sex_id year_id inc_* 
  
 merge m:1 location_id year_id using FILEPATH/speciesSplit.dta, assert(3) nogenerate

 forvalues i = 0 / 999 {
	foreach species in g r {
		quietly {
			generate total_`species'_`i' = inc_`i' * pr_`species'
			generate sleep_`species'_`i' = inc_sleep_`i' * pr_`species'
			generate disf_`species'_`i'  = inc_disf_`i' * pr_`species'
			}
		}
	di "." _continue
	}

 drop inc_*

 generate measure_id = 6
 
 append using FILEPATH/prevDraws.dta
 save FILEPATH/nonFatalDraws.dta, replace
