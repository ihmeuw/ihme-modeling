** ******************* NOW COMPUTE PREVALENCES *******************************
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
  
  merge m:1 location_id year_id using FILEPATH/speciesSplit.dta  

*drop kenya national 
drop if location_id==180
replace pr_g=0 if inlist(location_id, 179, 185, 193, 35620)
replace pr_g=1 if location_id==209
replace pr_r=1 if inlist(location_id, 179, 185, 193, 35620)
replace pr_r=0 if location_id==209
   

  
/* Generate 1000 draws of the splitting proportion for sequela (Severe motor and cognitive impairment due
   to sleeping disorder and disfiguring skin disease): 70%-74% split based on GBD 2010, which refers to Blum et al. 2006,
   who report on presence of symptoms at admission of patients in treatment centers. For treated cases, we assume that
   duration of sleeping disorder is about half of the total duration of treated cases.  */
   
  local A1 = 1884  // cases with sleeping disorder (Blum et.al. 2006)*/
  local A2 = 2533 - `A1'  // cases without sleeping disorder
  forvalues i = 0/999 {
    local a1 = rgamma(`A1',1)
    local a2 = rgamma(`A2',1)
    local prop_sleep_`i' = `a1' / (`a1' + `a2')  // implies: prop_sleep ~ beta(positives,negatives)
  }
  
/*
g-HAT:
Untreated: 3 years (then fatal)
Treated: 6 months (there is limited data on post-treatment period, but this seems reasonable given that treatment is a bit involved and involve hospitalization; it’s very hard to find a definitive source as it partly depends on the stage at the time of treatment)
 
r-HAT:
untreated: 6 months (then fatal)
Treated: 6 months  (same caveats as above)
*/

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

	
	
   replace reported_tgb=total_reported if missing(reported_tgb) & year_id==2017 & pr_g==1
   replace reported_tgb=0 if missing(reported_tgb) & year_id==2017 & pr_g==0
   
   replace reported_tgr=total_reported if missing(reported_tgr) & year_id==2017 & pr_r==1
   replace reported_tgr=0 if missing(reported_tgr) & year_id==2017 & pr_r==0
   
   *Uganda
   *apply % from 2016 to 2017
   replace reported_tgb=total_reported*.4 if location_id==190 & year_id==2017
   replace reported_tgr=total_reported*.6 if location_id==190 & year_id==2017 
	
* Estimate prevalence, based on reported cases (total_reported) and estimated undetected cases (deaths_*)
  *use `inc_deaths_country_year', clear
 
 drop new

* Add bogus years as copies of last year to allow the following loop to estimate prevalence for the last year.
  forvalues y = 2017/2021 {
    quietly expand 2 if year_id == `y', g(new)
    quietly replace year_id = `y' + 1 if new == 1
    quietly drop new
  }

/* Sum up prevalence of treated and untreated cases, assuming that untreated cases have been prevalent up to their death for a
   certain duration (stored in local "duration_#"). For untreated cases, we assume that halve the duration is spent with sleeping disorder
   (severe motor and cognitive impairment) and disfigurement (Checchi et al 2008). Treated (i.e. reported) cases are assumed to have
   been prevalent for 0.5 years, and for the fraction of treated cases that present with sleeping disorder, we assume that this is present
   for half the total duration and that the rest of the duration is spent suffering from disfiguring skin disease. Treated cases that don't
   present with sleeping disorder are assigned disfigurement for the entire duration. */
   
  sort location_id year_id
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
			count if missing(prev_total_`species'_`y') & year <= 2017
				if r(N) != 0 noisily di "Missing values of prev_total_`species'_`y'"
			count if missing(prev_sleep_`species'_`y') & year <= 2017
				if r(N) != 0 noisily di "Missing values of prev_sleep_`species'_`y'"
			count if missing(prev_disf_`species'_`y') & year <= 2017
				if r(N) != 0 noisily di "Missing values of prev_disf_`species'_`y'"
			}
		}	  
	di "." _continue
    }

 drop if year_id>2017
 keep location_id year_id prev_* 
 
  cross using FILEPATH/ageSexCurve.dta
  tempfile hat
  save `hat', replace
  
  
  
  foreach var in location year age_group {
	levelsof `var'_id, local (`var's) clean
	}

	
	
	
  get_population, location_id(`locations') age_group_id(`age_groups') sex_id(1 2) year_id(`years') clear
  
  merge 1:1 location_id age_group_id sex_id year_id using `hat', assert(3) nogenerate
  
  *order ihme_loc_id location* year_id age_group_id ageSex* sex_id population
  *sort ihme_loc_id year_id sex_id age_group_id
  
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

 *drop kenya national
 drop if location_id==180
 

replace pr_g=0 if inlist(location_id, 179, 185, 193, 35620)
replace pr_g=1 if location_id==209
replace pr_r=1 if inlist(location_id, 179, 185, 193, 35620)
replace pr_r=0 if location_id==209
 
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
 
 drop if !inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2017)
 
 save FILEPATH/nonFatalDraws.dta, replace
