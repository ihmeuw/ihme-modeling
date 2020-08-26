
/********************************************************************************\    
  Purpose: To make a complete covariate with population at risk for chagas. 

  Program Outline:
  1)  Data management: bring in, combine and clean up data; create needed vars
  2)  Identify countries with >1 year of population at risk data
  3)  Impute missing data for countries with >1 year of data; and estimate
      temporal trends
  4)  For countries with only 1 year of data, apply mean trend of its neighbors
  5)  Clean up the final dataset and save
  
do FILEPATH/ntd_chagas/00b_popAtRisk.do
\********************************************************************************/ 



/********************************************************************************\        
                              BOILERPLATE & SETUP 
\********************************************************************************/  
 
    clear all
    set more off
    
  if c(os) == "Unix" {
    local ADDRESS "FILEPATH"
    set odbcmgr ADDRESS
    }
  else if c(os) == "Windows" {
    local ADDRESS "FILEPATH"
    }


    run "FILEPATH"
    run "FILEPATH"
    
    
    local dirCustomChagas "FILEPATH"
    local dirCodemChagas  "FILEPATH"
    
    
    get_demographics, gbd_team(cod) clear
    local maxYear = max(`=subinstr("`r(year_id)'", " ", ",", .)')
    local minYear = min(`=subinstr("`r(year_id)'", " ", ",", .)')
    local nYears  = 1 + `maxYear' - `minYear'


/********************************************************************************\        
                                DATA MANAGEMENT 
\********************************************************************************/ 

  * PULL IN AND APPEND POPULATION AT RISK DATA *
    import delimited using "`dirCodemChagas'FILEPATH", clear
    tempfile appendTemp
    save `appendTemp'

    import delimited using "`dirCodemChagas'FILEPATH", clear
    append using `appendTemp'

    
  * CREATE UNIFORM PROPORTION AT RISK VARIABLE *
    gen prAtRisk = min(par/pop_paho, percent/100)


  * CLEAN UP DATASET *    
    keep yearstart countryiso3code parameter parametervalue prAtRisk
    drop if missing(prAtRisk) 
    drop if countryiso3code=="PRY" & yearstart==2005 

    rename country iso3 
    rename yearstart year

    tempfile data
    save `data', replace

    
  * BRING IN CHAGAS ENDEMCITY DATA & CREATE AN OBSERVATION FOR EACH YEAR AND COUNTRY *
    use "`dirCustomChagas'FILEPATH", clear
    keep if chagas==1
    
    expand `nYears'
        
    bysort iso3: gen year=_n+1979
        
    merge 1:1 iso3 year using `data'
    drop if _merge==2
    drop _merge
    
  * CREATE MEAN PROPORTION AT RISK VARIABLE *
    bysort iso3: egen meanPrAtRisk = mean(prAtRisk)
    

/********************************************************************************\        
               IDENTIFY COUNTRIES WITH >1 YEAR OF PR @ RISK DATA
\********************************************************************************/     
    
  * CREATE A VARIABLE THAT INDICATES IF THERE ARE ANY DATA FOR EACH COUNTRY & YEAR *
    bysort iso3 year: egen anyDataByObs = total(!missing(prAtRisk))
    bysort iso3 year: gen anyDataByYear = anyDataByObs>0 if _n==_N

  * CREATE A VARIABLE THAT INDICATES THE NUMBER OF YEARS OF DATA FOR EACH COUNTRY *    
    bysort iso3: egen nYearsData = total(anyDataByYear)    

  * GET LIST OF COUNTRIES WITH >1 YEAR DATA *    
    levelsof iso3 if nYearsData > 1, local(list) clean

    
    
/********************************************************************************\        
 IMPUTE MISSING DATA FOR COUNTRIES WITH >1 YR OF PR @ RISK DATA & ESTIMATE TRENDS
\********************************************************************************/
    
  * LOOP THROUGH COUNTRIES WITH >1 YEAR DATA & USE GLM TO IMPUTE PR @ RISK WHERE MISSING *
    foreach i of local list {
        quietly glm prAtRisk year if iso3=="`i'", link(logit) family(binomial) robust
        predict tempEst`i' if iso3=="`i'"
        }
    
  * COMBINE ALL COUNTRY SPECIFIC PREDICTIONS IN A SINGLE VARIABLE *
    egen prEst = rowmean(tempEst*)
    
  * CLEAN UP *    
    drop tempEst* anyData*
    
  * ESTIMATE COUNTRY-SPECIFIC TRENDS FOR COUNTRIES WITH >1 YEAR DATA * 
    bysort iso3 (year): gen trend = (prEst[_N] - prEst[1]) / `nYears'

    
/********************************************************************************\        
       APPLY TRENDS FROM NEIGHBORS TO COUNTRIES WITH ONLY 1 YEAR DATA
\********************************************************************************/
 
  * GET LIST OF COUNTRIES WITH ONLY 1 YEAR OF DATA *
    levelsof iso3 if nYearsData == 1, local(singles) clean
    
  * PULL IN LIST OF NEIGHBORING COUNTRIES *    
    preserve
    use "`dirCustomChagas'FILEPATH", clear

    foreach single of local singles {
        levelsof isoNeighbor if isoNode=="`single'", local(`single'Neigbors) clean
        }

    restore


  * LOOP THROUGH COUNTRIES WITH 1 YEAR OF DATA, GET & APPLY MEAN TREND OF NEIGHBORS *
    foreach single of local singles {
        gen neighborTemp = 0  // create a temporary working variable
    
        * Loop through and mark neighbors with >1 year of data *
        foreach neighbor of local `single'Neigbors {
            replace neighborTemp = 1 if iso3=="`neighbor'" & nYearsData>1
            }
            
        * Get mean trend of neigbors marked above *    
        quietly sum trend if neighborTemp==1
        
        * Apply that trend to the node country if any suitable neighbors exist *
        if `r(N)'>0    replace trend =`r(mean)' if iso3=="`single'"
        drop neighborTemp
    
        * Identify the year for which the node country has data *
        quietly sum year if iso3=="`single'" & !missing(prAtRisk)
    
        * Create estimate based on that country's 1 data point and neighbors' trends *
        replace prEst = meanPrAtRisk + trend * (year - `r(mean)') if iso3=="`single'"
        }


        
/********************************************************************************\        
                     BRING IN ISOs FROM NON-CHAGAS COUNTRIES
\********************************************************************************/
        
    keep iso3 year prEst
    rename prEst prAtRisk

    tempfile data 
    save `data', replace

     get_location_metadata, location_set_id(22) clear
    keep if is_estimate==1 | location_type=="admin0"  
    tab is_estimate if inlist(location_id, 44533, 4621, 4623, 4624, 4618, 4619, 4625, 4626, 4622, 4620, 4841, 4842, 4843, 4844, 4846, 4849, 4850, 4851, 4852, 4853, 4854, 4855, 4856, 4857, 4859, 4860, 4861, 4862, 4863, 4864, 4865, 4867, 4868, 4869, 4870, 4871, 4872, 4873, 4874, 4875, 44538, 44793, 44794, 44795, 44796, 44797, 44798, 44799, 44800)
     *44533, 44793, 44794, 44795, 44796, 44797, 44798, 44799, 44800
    
    keep location_id ihme_loc_id
    generate iso3 = ihme_loc_id
    
    expand `nYears'
    bysort location_id: gen year = _n + 1979
    
    merge m:1 iso3 year using `data'
    drop _merge
    
    save `data', replace

    

/********************************************************************************\        
                          FIX MEX SUBNATIONAL PROBLEM
\********************************************************************************/    
    *keep if sex=="male" & age=="1" 
    *drop age sex population
    tempfile prTemp
    save `prTemp', replace

    bysort year: egen temp = mean(prAtRisk) if substr(iso3, 1, 3)=="MEX"
    replace prAtRisk = temp if substr(iso3, 1, 3)=="MEX" & missing(prAtRisk)
    drop temp


    
/********************************************************************************\        
                        FINAL DATA PREP & CLEAN UP AND EXPORT
\********************************************************************************/
    
  * SET THE PR AT RISK TO ZERO FOR NON-ENDEMIC COUNTRIES *
    replace prAtRisk = 0 if missing(prAtRisk)

    rename year year_id
    save "`dirCustomChagas'FILEPATH", replace
    
    drop ihme_loc_id iso3
    
    generate covariate_id = 196
    generate covariate_name_short = "chagas_pop_at_risk2"
    generate age_group_id = 22
    generate sex_id = 3
    rename prAtRisk mean_value
    generate lower_value = mean_value
    generate upper_value = mean_value
    
    export delimited using FILEPATH, replace


