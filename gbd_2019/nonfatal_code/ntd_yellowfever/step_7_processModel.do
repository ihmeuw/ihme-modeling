
/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

*** BOILERPLATE ***
clear all
set maxvar 12000
set more off

*** PULL IN LOCATION_ID FROM BASH COMMAND ***
local location    "`1'"
local outDir      "`2'"
local deathsAlpha "`3'"
local deathsBeta  "`4'"
local endemic     "`5'"

*** START LOG ***
cap log using "FILEPATH", replace
if !_rc local close 1
else local close 0

di "`location'"
di "`outDir'"
di "`deathsAlpha'"
di "`deathsBeta'"
di "`endemic'"

run "FILEPATH"  
run "FILEPATH"  

*** SETUP A FEW LOCALS ***
tempfile appendTemp mergeTemp prevalence incidence chronic acute

local meid__asymp  ADDRESS
local meid_inf_mod ADDRESS
local meid_inf_sev ADDRESS
local meid_total   ADDRESS



/******************************************************************************\
           CREATE AND SAVE ZERO DRAW FILES IF LOCATION IS NON-ENDEMIC
\******************************************************************************/

if `endemic'==0 {

    get_demographics, gbd_team(cod) clear
    local ages  `r(age_group_id)'
    local sexes `r(sex_id)'
    local years `r(year_id)'
    local minYear = min(`=subinstr("`r(year_id)'", " ", ",", .)')
    local maxYear = max(`=subinstr("`r(year_id)'", " ", ",", .)')


    *** CREATE FILE OF ZERO DRAWS FOR NON-ENDEMIC LOCATIONS ***
    clear
    set obs `=wordcount("`ages'")'
    local i 1
    generate age_group_id = .
    foreach age of local ages {
        replace age_group_id = `age' in `i'
        local ++i
    }
        
    forvalues i = 0/999 {
      quietly generate draw_`i' = 0
    }
    
    expand 2, generate(sex_id)
    replace sex_id = sex_id + 1

    tempfile zeros
    save `zeros'

    clear
    set obs `=`maxYear' - `minYear' + 1'
    generate year_id = _n + `minYear' - 1

    cross using `zeros'

    generate cause_id = ADDRESS
    generate location_id = `location'
    export delimited using "FILEPATH", replace

    
    expand 2, gen(measure_id)
    replace measure_id = measure_id + 5
    generate modelable_entity_id = .
    drop cause_id 
    
    foreach state in _asymp inf_mod inf_sev total {    
        replace modelable_entity_id = `meid_`state''    
        export delimited using "FILEPATH", replace
    }
}

*** "if/else" statement continues with "else" brackets after next comment ***

/******************************************************************************\

                      CREATE DRAWS FOR ENDEMIC LOCATIONS

   For each draw we first create a draw for the age-sex distribution & correct
   to ensure that the proportions of cases in each age-sex group sums to one.
   
   We then create the incidence draw based on the raw case reports, for years
   and countries with non-missing reports, or model estimates where reports are
   missing (note that zero does not equal missing & vice versa)
   
   Finally, we estimate the number of deaths as the product of incidence, 
   case fatality and population.
   
\******************************************************************************/ 

else {

    *** LOAD INCIDENCE ESTIMATE FILE ***
    use "FILEPATH", clear


    *** PROCESS SPLITS ***
    local asympMu 0.55  
    local asympSigma = (0.74 - 0.37) / (2 * invnormal(0.975)) // SD derived from CI of above proportion (CI symmetry suggests non-transformed scale)
    local asympAlpha = `asympMu' * (`asympMu' - `asympMu' ^ 2 - `asympSigma' ^2) / `asympSigma' ^2 
    local asympBeta  = `asympAlpha' * (1 - `asympMu') / `asympMu'

    local modMu 0.33  
    local modSigma = (0.52 - 0.13) / (2 * invnormal(0.975)) // SD derived from CI of above proportion (CI symmetry suggests non-transformed scale)
    local modAlpha = `modMu' * (`modMu' - `modMu' ^ 2 - `modSigma' ^2) / `modSigma' ^2 
    local modBeta  = `modAlpha' * (1 - `modMu') / `modMu'

    local sevMu = logit(0.12)  // (CI symmetry on logit scale suggests this transformation)
    local sevSigma = (logit(0.26) - logit(0.05)) / (2 * invnormal(0.975)) // SD derived from CI of above proportion 


    forvalues i = 0/999 {
        local _asympPr  = rbeta(`asympAlpha', `asympBeta')
        local inf_modPr = rbeta(`modAlpha', `modBeta')
        local inf_sevPr = invlogit(rnormal(`sevMu', `sevSigma'))
        local deathsPr  = rbeta(`deathsAlpha', `deathsBeta')

        local correction = `_asympPr' + `inf_modPr' + `inf_sevPr'


        quietly {
            generate inc_`i' = exp(rnormal(predFixed, predFixedSe) + rnormal(predRandom, predRandomSe)) * allAgePop    // estimate total all-age national cases
            replace  inc_`i' = rnormal(mean, standard_error) * exp(rnormal(0, predRandomSe)) * allAgePop if !missing(cases) & inc_`i'<cases  // replace estimate with reported cases if report>estimate //cases>4
            replace  inc_`i' = inc_`i' * prAgeSex * (ef_`i' / ((1 - `_asympPr') / `correction')) * braSubPr_`i' / ageSexPop  // convert to age-specific subnational rates
            replace  inc_`i' = 0 if inc_`i'<0 | yfCountry==0 | ageSexPop==0 | missing(inc_`i')
            
            foreach state in _asymp inf_mod inf_sev {
                local `state'Pr = ``state'Pr' / `correction'
                generate `state'_`i' = inc_`i' * ``state'Pr' 
                replace `state'_`i' = 0 if missing(`state'_`i')
            }
    
            generate deaths_`i' = inf_sev_`i' * `deathsPr' * ageSexPop
            replace  deaths_`i' = 0 if missing(deaths_`i')

            *replace  deaths_`i' = deaths_`i'/ef_`i' if countryIso=="BRA" & year_id<2017

            capture assert inc_`i' >= 0
        }

        if _rc!=0 {
            di "NEGATIVE VALUES for draw_`i'!!!" 
            local errors `errors' draw_`i'<0
        }

        di "." _continue
    }

    *** ... still inside "else" brackets ***

    /******************************************************************************\
    
                                EXPORT DRAWS
    
       We first export death draws, looping through every combination of year, 
       iso3 and sex, and exporting the corresponding observations and death draws.
    
       We then collpase the data into quinquennial incidence draws, rename the 
       draws from inc_0 - inc_999 to draw_0 - draw_999, and multiply by an average
       10-day duration to derive prevalence.
    
       100% of prevalence is exported to the severe infection sequelae since we
       believe that surveillence is unlikely to correctly attribute mild & moderate
       cases to yellow fever.
    
    \******************************************************************************/ 

    *** EXPORT DEATH DRAWS ***
    drop ef_* braSubPr_*
    generate cause_id = ADDRESS
    capture rename deaths_* draw_*

    export delimited cause_id location_id year_id age_group_id sex_id draw_* using "FILEPATH", replace


    *** PROCESS AND EXPORT NON-FATAL DRAWS ***
    drop draw_* cause_id

    expand 2, generate(measure_id)
    replace measure_id = measure_id + 5

    /* We estimate prevalence (p=i*d) assuming mean 10-day duration (95%CI=6-14) 
       We generate duration with uncertainty using random draws from a gamma distribution
       We first define the parameters of that distribution and then loop through the draws to 
       convert each from incidence to prevalence    */

    local u = 10                 // mean duration of 10 days
    local s = 4/invnormal(0.975) // sd of duration defined to yield 6-14 day CI
    local a = `u'^2 / `s'^2      // here we define the shape parameter for a gamma distribution
    local b = `s'^2 / `u'        // here we define the scale parameter for a gamma distribution

    foreach var of varlist _asymp* inf_mod* inf_sev* {
        quietly {
            local duration = rgamma(`a', `b') / 365.25 
            replace `var' = `var' * `duration' if measure_id==5 
            capture assert `var' >= 0
        }

        if _rc!=0 {
            di "NEGATIVE VALUES for draw_`i'!!!" 
            local errors `errors' draw_`i'<0
        }

        di "." _continue
    }


    forvalues i = 0/999 {
        quietly generate total_`i' = _asymp_`i' + inf_mod_`i' + inf_sev_`i'
    }


    generate modelable_entity_id =  .

    foreach state in _asymp inf_mod inf_sev total {    
        rename `state'_* draw_*
        replace modelable_entity_id = `meid_`state''

        export delimited modelable_entity_id location_id year_id age_group_id sex_id measure_id draw_* using "FILEPATH", replace

        drop draw_* 
    }

}    //  close "else" brackets



file open progress using "FILEPATH", text write replace
file write progress "complete"
file close progress

if `close' log close
clear
