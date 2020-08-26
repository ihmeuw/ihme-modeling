// Purpose: Transform seroincid and seroprev estimates to incid and prev
//          of infection by severity
//          - Pulls seroincid and seroprev from DisMod
//******************************************************************************

/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 20000
  set more off

	
  run FILEPATH/get_draws.ado
 
  tempfile appendTemp mergeTemp adjust age_ids

* PULL IN LOCATION_ID AND INCOME CATEGORY FROM BASH COMMAND *  
  local location "`1'"  
  
* SET UP OUTPUT DIRECTORIES *  
  local outDir FILEPATH

* SET UP LOCALS WITH MODELABLE ENTITY IDS AND AGE GROUPS *  
  * sero of hep E
  local meid 1659
  local ages 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 164 235
  local sexes 1 2



/******************************************************************************\
                      PULL IN DRAWS AND MAKE CALCULATIONS
\******************************************************************************/
  
get_draws, gbd_id_type("modelable_entity_id") gbd_id(`meid') source("epi") location_id(`location') age_group_id(`ages') measure_id(6) status("best") gbd_round_id(6) decomp_step("step4") clear
rename draw_* inc_*

* Create new column in the last position of the working dataset
gen cause_id = 404

* replace age group 95-99 (group 33) with age group 95+ (group 235). 
destring age_group_id, replace
replace age_group_id = 235 if age_group_id==33


/*****************************************************************************
                    EXPORT FILES AND PERFORM SEQUELA SPLITS
*****************************************************************************/		

preserve

* PRODUCE INCIDENCE DRAWS *
*probability of acute infection by age from Armstrong & Bell,2002; DOI: 10.1542/peds.109.5.839

*median ages within age groups to be used in severity split formula
import delimited FILEPATH, clear
save `age_ids', replace
restore

merge m:1 age_group_id using `age_ids', nogen keep(3)


*export total (symp + asymp)
forvalues i = 0/999 {
  quietly {

    replace inc_`i' = 0 if missing(inc_`i')

    *generate prev by multiplying incid by 4 week duration
    generate t5_`i' = inc_`i' * (4 / 52)

    generate t6_`i' = inc_`i'

  }
}

* EXPORT TOTAL DRAWS *  
local years 1990 1995 2000 2005 2010 2015 2017 2019 

foreach parameter in 5 6 {

  rename t`parameter'_* draw_*
  foreach year in `years' {
    foreach sex in 1 2 {
    export delimited age_group_id draw_* using FILEPATH if year_id==`year' & sex_id==`sex', replace 
    }
  }

  rename draw_* t`parameter'_*
}



*severity splits*
generate prAcute   = logit(0.6 * (1 - exp(-0.011 * age_mid^1.86)))   

* standard error back calculated from Armstrong & Bell's CI's for prAcute in 10-17 year olds and is in logit space
generate prAcuteSe = .25 

local prSev    = 0.02/0.6  
local prSevSe  = `prSev' / 4
local alphaSev = `prSev' * (`prSev' - `prSev'^2 - `prSevSe'^2) / `prSevSe'^2 
local betaSev  = `alphaSev' * (1 - `prSev') / `prSev'
  
forvalues i = 0/999 {
  quietly {

    local random = rnormal(0,1)
  
    * estimate number of symptomatic infections
    generate sympTemp = invlogit(rnormal(prAcute, prAcuteSe)) * inc_`i' 
    * assign all birth prev to symptomatic group
    replace sympTemp = 0 if missing(sympTemp)     

    generate _asymp6_`i'  = inc_`i'  - sympTemp
    replace  _asymp6_`i' = 0 if missing(_asymp6_`i')
    generate inf_sev6_`i' = rbeta(`alphaSev', `betaSev') * sympTemp
    replace  inf_sev6_`i' = 0 if missing(inf_sev6_`i')
    generate inf_mod6_`i' = sympTemp - inf_sev6_`i'
    replace  inf_mod6_`i' = 0 if missing(inf_mod6_`i')
  
  rename sympTemp _symp6_`i'

  foreach seq in _asymp inf_mod inf_sev _symp {
    *durations specified in weeks, the 52 converts to years. Assuming 4 week duration.
    generate `seq'5_`i' = `seq'6_`i' * 4 / 52 

    }
  }
}


* EXPORT SEQUELA DRAWS * 
local years 1990 1995 2000 2005 2010 2015 2017 2019 

foreach parameter in 5 6 {
  foreach state in inf_mod inf_sev _asymp _symp {
    rename `state'`parameter'_* draw_*
    foreach year in `years' {
      foreach sex in 1 2 {
      export delimited age_group_id draw_* using FILEPATH if year_id==`year' & sex_id==`sex', replace 
     } 
   }
    rename draw_* `state'`parameter'_*
  }
}


