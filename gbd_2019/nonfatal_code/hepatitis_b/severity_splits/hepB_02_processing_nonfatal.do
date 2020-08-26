// Purpose: Transform seroincid and seroprev estimates to incid and prev
//          of infection by severity
//          - Pulls seroincid and seroprev from DisMod
// Input:   FILEPATH
// Output:  Datasets of incid and prev split by severity (csv, draws),
//          saved to the direcotry specified in outDir
//******************************************************************************

/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 32000
  set more off
  local username = c(username)
  
  tempfile appendTemp mergeTemp adjust

* PULL IN LOCATION_ID AND INCOME CATEGORY FROM BASH COMMAND *  
  local location "`1'"
  
* SET UP OUTPUT DIRECTORIES *  
  local outDir FILEPATH

* Seroprevalence of HBsAg
* SET UP LOCALS WITH MODELABLE ENTITY IDS AND AGE GROUPS *  
  local meid OBJECT

  *Generate ages, then tokenize to loop through values to replace in template
  local ages 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235
  local sexes 1 2

  local years 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019

  
/******************************************************************************\
                      PULL IN DRAWS AND MAKE CALCULATIONS
\******************************************************************************/
  
  run "FILEPATH/get_draws.ado"
  
    * PULL IN DRAWS FROM DISMOD MODELS FOR HBsAG PREVALENVCE AND INCIDENCE *
      get_draws, gbd_id_type("modelable_entity_id") gbd_id(`meid') source("epi") location_id(`location') age_group_id(`ages') measure_id(5) status("best") gbd_round_id(6) decomp_step("step4") clear
      rename draw_* prev_*
      save `mergeTemp'
    
    * Incidence, stick prev and incidence together
      get_draws, gbd_id_type("modelable_entity_id") gbd_id(`meid') source("epi") location_id(`location') measure_id(6) status("best") gbd_round_id(6) decomp_step("step4") clear
      rename draw_* incCarrier_*
      *drop n_draws
      merge 1:1 age_group_id year_id sex_id using `mergeTemp', gen(merge56)

      * save prevalence at birth as incidence at birth 
      forvalues i = 0/999 {
        quietly {
          replace incCarrier_`i' = prev_`i' if age_group_id == 164
        }
      }

      * replace age group 95-99 (group 33) with age group 95+ (group 235). Why do you need this if ages variable only includes 235? Can probably remove.
      destring age_group_id,replace
      replace age_group_id = 235 if age_group_id==33

* check 1 
  
    merge m:1 age_group_id using FILEPATH
    drop if _merge == 2

* check 2 

    gen cause_id = 402
   
  * CORRECT INCIDENCE (RESCALE INCIDENCE TO PREVALENCE WITH AGE PATTERN BASED ON PRE-VACCINATION ESTIAMTES) * 
    egen incCarrierMean = rowmean(incCarrier_*)
    egen prevMean = rowmean(prev_*)

    destring location_id, replace
    destring sex_id, replace
    destring year_id, replace
    sort location_id sex_id age_group_id year_id

*check 3
      *for location/sex/age, assuming that rows are sorted by year, create new variable inc1995 for every row where the year
      *is above 1995, and set it to the value of mean carrier incidence in the second row
      *divide prev by mean prev in the second row
      *by location_id sex_id age_group_id (year_id): gen inc1995   = incCarrierMean[2] if year_id>1995
      *by location_id sex_id age_group_id (year_id): gen prevRatio = prevMean / prevMean[2] if year_id>1995

      *export delimited using FILEPATH

  * PERFORM DRAW-LEVEL CALCULATIONS TO SPLIT TYPHOID & PARATYPHOID, & CALCULATE MRs *
    forvalues i = 0/999 {
      quietly {
        *by location_id sex_id age_group_id (year_id): replace incCarrier_`i' = inc1995 * prevRatio * incCarrier_`i' / incCarrierMean if year_id>1995
    
        *divide incidence by probability of becoming a carrier to convert incidence of carrier state to total incidence
        *rbeta generates random numbers from a beta distribution defined by a and b parameters
        *prCarrierAlpha and prCarrierBeta are created in chronic2acute file
        *inc is the incidence of total hepatitis B
        generate inc_`i' = (incCarrier_`i')  / rbeta(prCarrierAlpha, prCarrierBeta)  

        *multiply by the probability of incidence of SYMPTOMATIC acute infection
        generate acute_`i' = inc_`i' * rbeta(prAcuteAlpha, prAcuteBeta) 

      }
    }

* check 4 
/******************************************************************************\
                    EXPORT FILES AND PERFORM SEQUELA SPLITS
\******************************************************************************/    
    

  keep if (mod(year_id,5)==0 | year_id==2017 | year_id==2019) & year_id>=1990
  
  * EXPORT CHRONIC PREVALENCE DRAWS * 
  local years 1990 1995 2000 2005 2010 2015 2017 2019 

  * EXPORT CHRONIC INCIDENCE AND PREVALENCE *
  rename incCarrier_* draw_*

    foreach year in `years'{
      forvalues sex = 1/2 {
        export delimited age_group_id draw_* using FILEPATH if sex_id==`sex' & year_id==`year', replace
      }
    }
   
  drop draw_*
  
  rename prev_* draw_*

    foreach year in `years' {
      forvalues sex = 1/2 {
        export delimited age_group_id draw_* using FILEPATH if sex_id==`sex' & year_id==`year', replace
      }
    }
  drop draw_*
  capture drop incCarrier_* prev_*
  

  * EXPORT TOTAL ACUTE INCIDENCE AND PREVALENCE *
   rename inc_* draw_*

    foreach year in `years'{
      forvalues sex = 1/2 {
        export delimited age_group_id draw_* using FILEPATH if sex_id==`sex' & year_id==`year', replace
      }
    } 
  rename draw_* inc_*


  forvalues i = 0/999 {
      quietly generate draw_`i' = inc_`i' * 6 /52 
  }
  foreach year in `years'{
    forvalues sex = 1/2 {
      export delimited age_group_id draw_* using FILEPATH if sex_id==`sex' & year_id==`year', replace
    }
  }
   
  drop draw_*


  * EXPORT TOTAL SYMPTOMATIC INCIDENCE AND PREVALENCE *
  rename acute_* draw_*

    foreach year in `years'{
      forvalues sex = 1/2 {
        export delimited age_group_id draw_* using FILEPATH if sex_id==`sex' & year_id==`year', replace
      }
    } 
  rename draw_* acute_*

  forvalues i = 0/999 {
      quietly generate draw_`i' = acute_`i' * 6 /52 
  }
  foreach year in `years'{
    forvalues sex = 1/2 {
      export delimited age_group_id draw_* using FILEPATH if sex_id==`sex' & year_id==`year', replace
    }
  }
   
  drop draw_*




  * SET UP SEQUELA SPLIT *
    expand 3
    bysort age_group_id year_id sex_id: generate index = _n

    * creates three rows for every unique combo of age/year/sex, one assigned to each severity
    generate state = "inf_mod"  if index==1
    replace state = "inf_sev"  if index==2
    replace state = "_asymp"   if index==3
   
   
   
    * SPLIT OUT INCIDENCE *
    *  acute is the incidence of acute hep infection 
    *  severe cases: multiply acute incid by the prob of severe
    *  moderate cases: subtract the severe cases from the acute incid
    *  asymptomatic: subtract the acute incid (symptomatic) from the total incid

    * The local prSev line pulls the proportion of acute cases that are severe from a beta distribution, based on numbers from McMahon et al
    forvalues i = 0 /999 {
      quietly {
         local prSev = rbeta(7, 19)  
         generate draw_`i' = acute_`i' * `prSev'  if state=="inf_sev" | state=="inf_mod"
         replace  draw_`i' = acute_`i' - draw_`i' if state=="inf_mod"
         replace  draw_`i' = inc_`i' - acute_`i'  if state=="_asymp"
      }
    }
    
    drop acute_* inc_*
    
   
      * EXPORT SEQUELA INCIDENCE DRAWS *
    foreach state in inf_mod inf_sev _asymp {
      foreach year in `years' {
        forvalues sex = 1/2 {
          export delimited age_group_id draw_* using FILEPATH if state=="`state'" & sex_id==`sex' & year_id==`year', replace
        }
      }
    }
      
      
    * CALCULATE PREVALENCE *  
    * multiply incidence by the fraction of the year that is the duration six weeks
    forvalues i = 0/999 {
        quietly replace draw_`i' = draw_`i' * 6 /52 
      }
      
      
    * EXPORT SEQUELA PREVALENCE DRAWS * 
    foreach state in inf_mod inf_sev _asymp {
    foreach year in `years' {
          forvalues sex = 1/2 {
            export delimited age_group_id draw_* using FILEPATH if state=="`state'" & sex_id==`sex' & year_id==`year', replace
     }
  }
}
