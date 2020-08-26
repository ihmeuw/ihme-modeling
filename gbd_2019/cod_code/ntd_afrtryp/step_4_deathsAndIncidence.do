*** ======================= BOILERPLATE ======================= ***
clear all
set more off
set maxvar 32000
if c(os) == "Unix" {
    global prefix FILEPATH
    set odbcmgr ADDRESS
    }
else if c(os) == "Windows" {
    // Compatibility paths for Windows STATA GUI development
    global prefix FILEPATH
    local 2 FILEPATH
    local 3 "--draws"
    local 4 FILEPATH
    local 5 "--interms"
    local 6 FILEPATH
    local 7 "--logs"
    local 8 FILEPATH
}

// define locals from jobmon task
local params_dir        `2'
local draws_dir         `4'
local interms_dir       `6'
local logs_dir          `8'

cap log using "`logs_dir'/FILEPATH", replace
if !_rc local close_log 1
else local close_log 0

di "`params_dir'"
di "`draws_dir'"
di "`interms_dir'"
di "`logs_dir'"

// Load shared functions
adopath + FILEPATH



*** ======================= MAIN EXECUTION ======================= ***

*** ESTABLISH LOCALS & TEMPFILES ***
get_demographics, gbd_team(epi) clear
local currentYear =  word("`r(year_id)'", -1)

*** IMPORT DATA & PREP DATA ***
use "`interms_dir'/FILEPATH", clear

sort location_id year_id

*location_ids 179, 193, 213, 522 are missing entire 1990-2019 set
expand 32 if inlist(location_id, 179, 193, 213, 522) & year_id==1989, gen(new)
bysort location_id (year_id) : gen Nyear = sum(new[_n - 1]) 
replace year_id=year_id+Nyear if year_id==1989 & inlist(location_id, 179, 193, 213, 522) & new==1
drop if new==0 & inlist(location_id, 179, 193, 213, 522) & year_id==1989
drop Nyear new

*185 - 1990:2015
expand 28 if inlist(location_id, 185) & year_id==1989, gen(new)
bysort location_id (year_id) : gen Nyear = sum(new[_n - 1]) 
replace year_id=year_id+Nyear if year_id==1989 & inlist(location_id, 185) & new==1
drop if new==0 & inlist(location_id, 185) & year_id==1989
drop Nyear new

*209 - 1990:2016
expand 29 if inlist(location_id, 209) & year_id==1989, gen(new)
bysort location_id (year_id) : gen Nyear = sum(new[_n - 1]) 
replace year_id=year_id+Nyear if year_id==1989 & inlist(location_id, 209) & new==1
drop if new==0 & inlist(location_id, 209) & year_id==1989
drop Nyear new

drop if year_id==1977
drop if year_id==1978
drop if year_id==1979

drop if inlist(location_id, 175, 206, 210, 216, 35620)


*** PREP DATA TO PULL CDR DRAWS ***  
* Fill gaps in total number of reported cases
* For Mozambique, assume zero cases if missing
replace total_reported = 0 if inlist(location_id, 179, 185, 193, 209, 213, 522, 217) & missing(total_reported)


******modeling to replace 0s cases using poisson model for years pre-1990
tempfile master
save `master'

*remove locations with missing value_ppl_risk (prevents model convergence)
*remove years 2018 and 2019
keep if location_id==179 | location_id==185 | location_id==193 | location_id==209 | location_id==213 | year_id==2018 | year_id==2019

tempfile to_append_later
save `to_append_later'  

use `master'
drop if value_ppl_risk==.| year_id==2018 | year_id==2019 


levelsof location_id, local(locations) clean
    
    generate predicted = .
    generate predictedSe=.
    generate ln_size=ln(value_ppl_risk)
    
    local failed 
    
    foreach location of local locations {
        quietly poisson total_reported year_id if location_id==`location', offset(ln_size) 
        if `e(converged)'==1  {
            quietly {
                predict temp  if location_id==`location', xb nooffset
                predict tempSe if location_id==`location', stdp nooffset
                
                replace predicted = temp  if location_id==`location'
                replace predictedSe = tempSe  if location_id==`location'

                drop temp tempSe
                }
        di "." _continue
        }
        else {
            local failed `failed' `location'
            di _n "`location' failed to converge"
        }
    }

    gen exp_predict=.
    replace exp_predict=exp(predicted)
    gen new_case_counts=.
    replace new_case_counts=exp_predict*value_ppl_risk
    
    *replace all those with 0s and year_id<1990
    replace total_reported=new_case_counts if total_reported==0 & year_id<1990
    
    drop predicted
    drop predictedSe
    drop ln_size
    drop exp_predict
    drop new_case_counts
    
    append using `to_append_later' 
    sort location_id year_id


*recalculate ln_inc_risk using updated total reported/value_ppl_rirk
replace incidence_risk=total_reported/value_ppl_risk
replace ln_inc_risk=ln(incidence_risk)


save "`interms_dir'/FILEPATH", replace
use  "`interms_dir'/FILEPATH"


*** RUN MODEL TO ESTIMATE CDR ***   
mixed ln_inc_risk value_ln_coverage || location_id: 

predict random, reffects
predict randomSe, reses

sum random if e(sample)
local seMean = `r(sd)'
sum randomSe if e(sample)
replace randomSe = sqrt(`seMean'^2 + `r(mean)'^2) if missing(randomSe)


* Exponential interpolation of missing incidence rates 
*    between first and last year for which cases are reported
*    (roc = rate of change)
generate has_data = !missing(incidence_risk) 
bysort location_id has_data (year_id): generate double roc = (incidence_risk[_n+1]/incidence_risk)^(1/(year_id[_n+1]-year_id))
bysort location_id (year_id): replace roc = roc[_n-1] if missing(roc) & location_id
bysort location_id (year_id): replace incidence_risk = incidence_risk[_n-1] * roc[_n-1] if missing(incidence_risk) 

* carry backward rate for years with missing data before first reports of cases
gsort location_id -year_id
bysort location_id: replace incidence_risk = incidence_risk[_n-1] if missing(incidence_risk)
sort location_id year_id

replace total_reported = incidence_risk * value_ppl_risk if missing(total_reported)

* Fill gaps in population screening coverage (needed to predict case detection rate)
* Carry forward and backward over years within countries.
bysort location_id (year_id): replace value_ln_coverage = value_ln_coverage[_n-1] if missing(value_ln_coverage)
gsort location_id -year_id
bysort location_id: replace value_ln_coverage = value_ln_coverage[_n-1] if missing(value_ln_coverage)
sort location_id year_id
* For countries without any coverage data, assume the average of the region (on log scale)
bysort region_id year_id (location_id): egen mean_ln_cov = mean(value_ln_coverage)
sort region_id location_id year_id
* Regions without any coverage data at this point, assume the average over all other regions (on log scale)
bysort year_id (location_id): egen mean_mean_ln_cov = mean(mean_ln_cov)
sort location_id year_id
* Fold in all estimates
replace mean_ln_cov = mean_mean_ln_cov if missing(mean_ln_cov)
replace value_ln_coverage = mean_ln_cov if missing(value_ln_coverage)

save "`interms_dir'/FILEPATH", replace

* Generate 1000 draws of mortality among treated cases, assuming that 0.7% - 6.0% of all treated (reported) cases
* die (95%-CI; source: GBD 2010, which refers to Balasegaram 2006, Odiit et al. 1997, and Priotto et al. 2009)
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


* Start with predicting the counterfactual incidence, and simplify by shuffling terms.
* ln_inc_counterfact = ln_inc_risk - ln_coverage * b_ln_coverage[`j'], 
* cdr = exp(ln_inc_risk)/exp(ln_inc_counterfact)
* cdr = exp(ln_inc_risk - ln_inc_counterfact)
* cdr = exp(ln_inc_risk - (ln_inc_risk - ln_coverage * b_ln_coverage[`j']))
* cdr = exp(ln_inc_risk - ln_inc_risk + ln_coverage * b_ln_coverage[`j'])

local counter = 0

forvalues j = 1/1000 {
    quietly generate cdr_`counter' = exp(value_ln_coverage * b_value_ln_coverage[`j']) 

    * Generate "true" number of incident cases and deaths, but save reported and undetected cases separately,
    * so that we can apply different duration of symptoms for prevalence calculation.
    quietly {
        generate undetected_`counter' = (total_reported / cdr_`counter' - total_reported) * exp(rnormal((randomSe^2/-2), randomSe))
        generate total_`counter'  = total_reported + undetected_`counter'
        generate deaths_`counter' = undetected_`counter' + total_reported * mort_treated[`j']
    }

    local counter = `counter' + 1
}


*** CLEAN & SAVE ALL-AGE DRAWS OF CASES AND DEATHS ***
* Drop excess empty rows that were generated to hold draws of detection rates and
* excess rows related to the generation of draws
drop if missing(location_id)

keep ihme_loc_id location_name location_id parent_id region_id region_name year_id deaths_* undetected_* cdr_* total* 
order location_id year_id deaths_*

foreach stub in cdr undetected deaths {
    egen `stub'Mean = rowmean(`stub'_*)
}

replace ihme_loc_id="KEN_35620" if location_id==180
replace location_id=35620 if ihme_loc_id=="KEN_35620"


*please note: using all data, even pre-1980 data, and not applying any geo-resrictions

save "`interms_dir'/FILEPATH", replace


*** SPLIT OUT ALL-AGE ESTIMATES TO PRODUCE AGE/SEX-SPECIFIC ESTIMATES OF CASES AND DEATHS ***
cross using "`interms_dir'/FILEPATH"
tempfile hat
save `hat', replace

foreach var in location year age_group {
    levelsof `var'_id, local (`var's) clean
}

get_population, location_id(`locations') age_group_id(`age_groups') sex_id(1 2) year_id(`years') decomp_step("step4") clear
keep age_group_id location_id year_id sex_id population

merge 1:1 location_id age_group_id sex_id year_id using `hat', assert(3) nogenerate

order ihme_loc_id location* year_id age_group_id ageSex* sex_id population
sort ihme_loc_id year_id sex_id age_group_id

forvalues i = 0/999 {
    quietly {
        generate temp1 = exp(rnormal(ageSexCurve, ageSexCurveSe)) * population
        replace  temp1 = 0 if age_group_id <= ADDRESS
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


** NOW SPIT OUT INCIDENCE & DEATH DRAWS (CREATE ZERO DRAW FILES FOR NON-ENDEMIC LOCATIONS) ** 
/* Generate 1000 draws of the splitting proportion for sequela (Severe motor and cognitive impairment due to sleeping disorder and disfiguring skin disease): 70%-74% split based on GBD 2010, which refers to Blum et al. 2006, who report on presence of symptoms at admission of patients in treatment centers. For treated cases, we assume that duration of sleeping disorder is about half of the total duration of treated cases.
*/
local A1 = 1884    // cases with sleeping disorder (Blum et.al. 2006)
local A2 = 2533 - `A1'    // cases without sleeping disorder

forvalues i = 0/999 {
    local a1 = rgamma(`A1',1)
    local a2 = rgamma(`A2',1)
    local prop_sleep_`i' = `a1' / (`a1' + `a2') // implies: prop_sleep ~ beta(positives,negatives)
}

* Generate 1000 draws of total duration of symptoms in untreated cases (based on Checchi 2008 BMC Inf Dis)
local mean = 1026 / 365    // average total duration in years
local lower = 702 / 365
local upper = 1602 / 365
local sd = (ln(`upper')-ln(`lower'))/(invnormal(0.975)*2)
local mu = ln(`mean') - 0.5*`sd'^2    // the mean of a log-normal distribution = exp(mu + 0.5*sigma^2)

forvalues x = 0/999 {
    local duration_`x' = exp(rnormal(`mu',`sd'))
}

* Calculate number of incident cases based on all incident cases; assume same proportion in treated and unterated cases 
forvalues y = 0/999 {
    *quietly generate inc_total_`y' = draw_`y'
    quietly generate inc_sleep_`y' = `prop_sleep_`y'' * inc_`y'
    quietly generate inc_disf_`y' = inc_`y' - inc_sleep_`y'
}

sort location_id year_id age_group_id sex_id
keep ihme_loc_id location_id year_id age_group_id sex_id inc_* deaths_* undet*_* population



save "`interms_dir'/FILEPATH", replace


*** ======================= CLOSE LOG ======================= ***
if `close_log' log close

exit, STATA clear
