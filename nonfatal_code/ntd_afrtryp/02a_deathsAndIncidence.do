
	
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


	run "FILEPATH/fastcollapse.ado"	
	run "FILEPATH/get_demographics.ado"
	run "FILEPATH/get_population.ado"
	run "FILEPATH/get_location_metadata.ado"
	run "FILEPATH/get_envelope.ado"

*** ESTALISH LOCALS & TEMPFILES ***
	local inDir FILEPATH
	local make_graphs = 0
	

*** IMPORT DATA & PREP DATA ***
	use FILEPATH/data2Model.dta, clear
	expand 2 if year_id==2015, gen(newObs)
	replace year_id = year_id + newObs
	drop newObs 

	
*** RUN MODEL TO ESTIMATE CDR ***	
	mixed ln_inc_risk ln_coverage || ihme_loc_id:	

	predict random, reffects
	predict randomSe, reses

	sum random if e(sample)
	local seMean = `r(sd)'
	sum randomSe if e(sample)
	replace randomSe = sqrt(`seMean'^2 + `r(mean)'^2) if missing(randomSe)



*** PREP DATA TO PULL CDR DRAWS ***  
    replace total_reported = 0 if ihme_loc_id == "MOZ" & missing(total_reported)
	replace total_reported = 0 if inlist(ihme_loc_id, "BWA", "ETH", "GNB", "RWA", "SLE") & missing(total_reported)
	bysort ihme_loc_id (year_id): replace incidence_risk = incidence_risk[_n-1] if year == 2015 & missing(incidence_risk)
	
  // Exponential interpolation of missing incidence rates between first and last year for which cases are reported (roc = rate of change)
    generate has_data = !missing(incidence_risk)
    bysort ihme_loc_id has_data (year_id): generate double roc = (incidence_risk[_n+1]/incidence_risk)^(1/(year_id[_n+1]-year_id))
    bysort ihme_loc_id (year_id): replace roc = roc[_n-1] if missing(roc)
    bysort ihme_loc_id (year_id): replace incidence_risk = incidence_risk[_n-1] * roc[_n-1] if missing(incidence_risk)
	

    gsort ihme_loc_id -year_id
    bysort ihme_loc_id: replace incidence_risk = incidence_risk[_n-1] if missing(incidence_risk)
    sort ihme_loc_id year_id

    replace total_reported = incidence_risk * ppl_risk if missing(total_reported)


    bysort ihme_loc_id (year_id): replace ln_coverage = ln_coverage[_n-1] if missing(ln_coverage)
    gsort ihme_loc_id -year_id
    bysort ihme_loc_id: replace ln_coverage = ln_coverage[_n-1] if missing(ln_coverage)
    sort ihme_loc_id year_id
    bysort region_name year_id (ihme_loc_id): egen mean_ln_cov = mean(ln_coverage)
    sort region_name ihme_loc_id year_id
    bysort year_id (ihme_loc_id): egen mean_mean_ln_cov = mean(mean_ln_cov)
    sort ihme_loc_id year_id
    replace mean_ln_cov = mean_mean_ln_cov if missing(mean_ln_cov)
    replace ln_coverage = mean_ln_cov if missing(ln_coverage)
  
// Generate 1000 draws of mortality among treated cases (Balasegaram 2006, Odiit et al. 1997, and Priotto et al. 2009)
  local sd_mort_treat = (log(0.06) - log(0.007)) / (invnormal(0.975) * 2)
  local mu_mort_treat = (log(0.06) + log(0.007)) / 2
  capture set obs 1000
  generate mort_treated = exp(`mu_mort_treat' + rnormal() * `sd_mort_treat')

  
  
*** Generate 1000 draws of case detection rate and counterfactual cases, given (expected) screening coverage ***
	matrix m = e(b)'
	matrix m = m[1..2,1]
	
	local covars: rownames m
	local num_covars: word count `covars'
	local betas
	
	forvalues j = 1/`num_covars' {
		local this_covar: word `j' of `covars'
		local betas `betas' b_`this_covar'
		}
		
	matrix C = e(V)
	matrix C = C[1..2,1..2]
	drawnorm `betas', means(m) cov(C)

	// Start with predicting the counterfactual incidence, and simplify by rearranging terms.
	// Assuming everything is the same (random effects, residuals, etc.), the counterfactual log-incidence is:
	//    ln_inc_counterfact = ln_inc_risk - ln_coverage * b_ln_coverage[`j'], 
	//    cdr = exp(ln_inc_risk)/exp(ln_inc_counterfact)
	//    cdr = exp(ln_inc_risk - ln_inc_counterfact)
	//    cdr = exp(ln_inc_risk - (ln_inc_risk - ln_coverage * b_ln_coverage[`j']))
	//    cdr = exp(ln_inc_risk - ln_inc_risk + ln_coverage * b_ln_coverage[`j'])
	
	local counter = 0
	
	forvalues j = 1/1000 {
		quietly generate cdr_`counter' = exp(ln_coverage * b_ln_coverage[`j']) 

		quietly {
			generate undetected_`counter' = (total_reported / cdr_`counter' - total_reported) * exp(rnormal((randomSe^2/-2), randomSe))
			generate total_`counter'  = total_reported + undetected_`counter'
			generate deaths_`counter' = undetected_`counter' + total_reported * mort_treated[`j']
			}
				   
		local counter = `counter' + 1
		}


	
*** CLEAN UP DATASET & SAVE ALL-AGE DRAWS OF CASES AND DEATHS ***
		drop if missing(ihme_loc_id)
	  
	keep ihme_loc_id location_name location_id parent_id region_name year_id deaths_* undetected_* cdr_* total* 
	order ihme_loc_id year_id deaths_*

	foreach stub in cdr undetected deaths {
	egen `stub'Mean = rowmean(`stub'_*)
	}
		
	save FILEPATH/deathAndCaseDraws.dta, replace
	

	
*** SPLIT OUT ALL-AGE ESTIMATES TO PRODUCE AGE/SEX-SPECIFIC ESTIMATES OF CASES AND DEATHS ***
	cross using FILEPATH/ageSexCurve.dta
	tempfile hat
	save `hat', replace

	foreach var in location year age_group {
		levelsof `var'_id, local (`var's) clean
		}

	get_population, location_id(`locations') age_group_id(`age_groups') sex_id(1 2) year_id(`years') clear
	drop process_version_map_id
	merge 1:1 location_id age_group_id sex_id year_id using `hat', assert(3) nogenerate

	order ihme_loc_id location* year_id age_group_id ageSex* sex_id population
	sort ihme_loc_id year_id sex_id age_group_id

	forvalues i = 0/999 {
		quietly {
			generate temp1 = exp(rnormal(ageSexCurve, ageSexCurveSe)) * population
			replace  temp1 = 0 if age_group_id <= 4
			bysort location_id year_id: egen temp2 = total(temp1)
			replace deaths_`i' = deaths_`i' * temp1 / temp2
			replace undetected_`i' = undetected_`i' * temp1 / temp2
			replace total_`i' = total_`i' * temp1 / temp2
			generate inc_`i' = total_`i' / population
			generate undet_inc_`i' = undetected_`i' / population
			drop temp1 temp2
			}
		di "." _continue
		}

	
* NOW SPIT OUT INCIDENCE & DEATH DRAWS (REMEMBER TO CREATE ZERO DRAW FILES FOR NON-ENDEMIC LOCATIONS) *	
	local A1 = 1884  // cases with sleeping disorder (Blum et.al. 2006)
	local A2 = 2533 - `A1'  // cases without sleeping disorder
	
	forvalues i = 0/999 {
		local a1 = rgamma(`A1',1)
		local a2 = rgamma(`A2',1)
		local prop_sleep_`i' = `a1' / (`a1' + `a2')  // implies: prop_sleep ~ beta(positives,negatives)
		}
	  
	// Generate 1000 draws of total duration of symptoms in untreated cases (based on Checchi 2008 BMC Inf Dis)
	local mean = 1026 / 365  // average total duration in years
	local lower = 702 / 365
	local upper = 1602 / 365
	local sd = (ln(`upper')-ln(`lower'))/(invnormal(0.975)*2)
	local mu = ln(`mean') - 0.5*`sd'^2  // the mean of a log-normal distribution = exp(mu + 0.5*sigma^2)

	forvalues x = 0/999 {
		local duration_`x' = exp(rnormal(`mu',`sd'))
		}

	forvalues y = 0/999 {
		quietly generate inc_sleep_`y' = `prop_sleep_`y'' * inc_`y'
		quietly generate inc_disf_`y' = inc_`y' - inc_sleep_`y'
		}

	sort ihme_loc_id year_id age_group_id sex_id
	keep ihme_loc_id location_id year_id age_group_id sex_id inc_* deaths_* undet*_* population

	save FILEPATH/incAndDeathDraws.dta, replace


