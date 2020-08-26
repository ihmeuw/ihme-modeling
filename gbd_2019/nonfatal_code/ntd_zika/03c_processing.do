* do FILEPATH/nts_zika/03c_processing.do 

/******************************************************************************\
            SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

*** BOILERPLATE ***
clear all
set maxvar 10000
set more off

if c(os) == "Unix" {
    local ADDRESS "FILEPATH"
    set odbcmgr ADDRESS
    }
else if c(os) == "Windows" {
    local ADDRESS "FILEPATH"
    }

local location `1'

cap log using "FILEPATH", replace

adopath + FILEPATH
adopath + FILEPATH 
run FILEPATH
run FILEPATH

tempfile remission appendTemp



*** SETUP DIRECTORIES ***
local rootDir FILEPATH



*** CREATE EMPTY ROWS FOR INTERPOLATION ***
get_demographics, gbd_team(epi) clear
local years `r(year_id)'
local ages`r(age_group_id)'

clear
set obs `=`=word("`r(year_id)'", -1)'-1979'
generate year_id = _n + 1979    
*want to drop estimation years
drop if (mod(year_id,5)==0 & inrange(year_id, 1990, 2015)) | year_id==2017 | year_id==2019


generate age_group_id = ADDRESS1
foreach age of local ages {
    expand ADDRESS1 if age_group_id==ADDRESS1, gen(newObs)
    replace age_group_id = `age' if newObs==1
    drop newObs
    }
replace age_group_id = ADDRESS2 if age_group_id== ADDRESS3

bysort year_id age_group_id: generate sex_id = _n

save `appendTemp'



*** GET GUILLIAN-BARRE REMISSION DRAWS ***
get_draws, gbd_id_type(modelable_entity_id) source(epi) gbd_id(ADDRESS) location_id(`location') age_group_id(`ages') measure_id(7) status(best) gbd_round_id(6) decomp_step("step3") clear

rename draw_* remission_*
drop modelable_entity_id model_version_id measure_id

append using `appendTemp'

fastrowmean remission_*, mean_var_name(remissionMean)    

forvalues year = 1980/2016 {
    local index = `year' - 1979

    if `year'< 1990{
        local indexStart = 1990 - 1979
        local indexEnd = 2019 - 1979
        }
    else if `year'==2016 {
        local indexStart = 36
        local indexEnd = 38
        }
    else {
        local indexStart = 5 * floor(`year'/5) - 1979
        local indexEnd = 5 * ceil(`year'/5) - 1979
        if `indexStart'==`indexEnd' continue
        }

    foreach var of varlist remission_* {
        quietly {
            bysort age_group_id sex_id (year_id): replace `var' = `var'[`indexStart'] * exp(ln(remissionMean[`indexEnd']/remissionMean[`indexStart']) * (`index'-`indexStart') / (`indexEnd'-`indexStart')) if year_id==`year'
            replace `var' = 0 if missing(`var') & year_id==`year'
            }
        di "." _continue
        }
    }

local index_2018 = 39
local indexStart_2018 = 38
local indexEnd_2018 = 40

foreach var of varlist remission_* {
    quietly {
        bysort age_group_id sex_id (year_id): replace `var' = `var'[`indexStart_2018'] * exp(ln(remissionMean[`indexEnd_2018']/remissionMean[`indexStart_2018']) * (`index_2018'-`indexStart_2018') / (`indexEnd_2018'-`indexStart_2018')) if year_id==2018
        replace `var' = 0 if missing(`var') & year_id==2018
        }
    di "." _continue
    }

replace location_id = `location'

save `remission'

use FILEPATH, clear

replace efAlpha = 4.817826 if location_id==422 
replace efBeta  = 9.259581 if location_id==422



*** CREATE ALL-AGE CASE DRAWS, ADJUSTING FOR UNDERREPORTING ***
replace imported = 0 if missing(imported) | countryCases==0
forvalues i = 0/999 {
    generate gammaA = rgamma(efAlpha, 1)
    generate gammaB = rgamma(efBeta, 1)
    quietly generate draw_`i' = ((rbinomial(sample_size, ((cases + imported)/sample_size)) / (gammaA / (gammaA + gammaB))) * (pop_all_age / sample_size)) 
    drop gammaA gammaB
    }



*** APPLY AGE/SEX PATTERNS TO CALCULATE AGE/SEX-SPECIFIC CASE DRAWS ***
merge 1:m location_id year_id using FILEPATH, assert(3) nogenerate    // age-specific by location

forvalues i = 0/999 {
    quietly {
        generate temp1 = exp(rnormal(ageSexCurve, ageSexCurveSe)) * pop_age_specific
        bysort location_id year_id: egen temp2 = total(temp1)
        replace draw_`i' = draw_`i' * temp1 / temp2
        drop temp1 temp2
        }
    di "." _continue
    }



*** APPLY OUTCOMES SPLIT ***
cross using FILEPATH

forvalues i = 0/999 {
    quietly {
        replace draw_`i' = draw_`i' * multiplier if outcome=="inf_mod"
        replace draw_`i' = (draw_`i' / rbeta(alpha, beta)) - draw_`i' if outcome=="_asymp"
        replace draw_`i' = draw_`i' * prPreg / rbeta(alpha, beta) if outcome=="preg" 
        replace draw_`i' = draw_`i' * rbeta(alpha, beta) if inlist(outcome, "fatalities", "gbs")
        replace draw_`i' = 0 if missing(draw_`i')
        }
    di "." _continue
    }



*** EXPORT DRAWS FOR NON-FATAL OUTCOMES OTHER THAN PREG ***
preserve
collapse (sum) draw_* pop_age_specific, by(location_id year_id age_group_id sex_id outcome modelable_entity_id)

expand 2, generate(measure_id)
replace measure_id = measure_id + 5

merge m:1 location_id year_id age_group_id sex_id using `remission', assert(2 3) keep(3) nogenerate

** Use remission draws from Guillian-Barre envelope DisMod model to estimate duration of Zika GBS
forvalues i = 0/999 {
    quietly {
        replace draw_`i' = draw_`i' / pop_age_specific
        replace draw_`i' = 0 if missing(draw_`i')
        * Using dengue duration as approximation: Whitehead et al, doi: 10.1038/nrmicro1690
        replace draw_`i' = draw_`i' * 6/365 if measure_id==5 & inlist(outcome, "_asymp", "inf_mod") 
        replace draw_`i' = draw_`i' / remission_`i' if measure_id==5 & outcome=="gbs" 
        }
    }

drop if measure_id==6 & outcome=="gbs" 

foreach outcome in gbs _asymp inf_mod {
    export delimited location_id year_id age_group_id sex_id measure_id modelable_entity_id draw_* using FILEPATH if outcome=="`outcome'", replace
    }

keep if inlist(outcome, "_asymp", "inf_mod") & measure_id==6
collapse (sum) draw_*, by(location_id year_id age_group_id sex_id measure_id)
generate modelable_entity_id = ADDRESS
export delimited location_id year_id age_group_id sex_id measure_id modelable_entity_id draw_* using FILEPATH, replace



*** ESTIMATE BIRTHS TO WOMEN INFECTED WITH ZIKA IN FIRST TRIMESTER ***
restore
keep if outcome=="preg" & sex_id==2
collapse (sum) draw_*, by(location_id year_id)

merge 1:m location_id year_id using FILEPATH    // prBirthsBySex by location

expand 2 if year_id==1980, gen(newObs)
replace year_id = year_id - newObs
sort location_id year_id


** Here we apply a shift to account for lag between 1st trimester infecion and actual date of birth
** 24 weeks is min gest age for which fetal growth standards exist -- consider this min gest age for CZS 
** https://www.cdc.gov/zika/hc-providers/infants-children/zika-syndrome-birth-defects.html
forvalues i = 0/999 {
    quietly {
        bysort sex_id (location_id year_id): gen temp = draw_`i'[_n-1]
        replace draw_`i' = ((draw_`i' * (52-24)/52) + (temp * 43/52)) * prBirthsBySex
        replace draw_`i' = 0 if missing(draw_`i')
        drop temp
        }
    }
    
drop if year_id==1979
generate age_group_id = ADDRESS2

export delimited location_id year_id age_group_id sex_id draw_* using FILEPATH, replace


file open progress using FILEPATH.txt, text write replace
file write progress "complete"
file close progress

log close
