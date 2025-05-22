// Purpose: Transform seroincid and seroprev estimates to incid and prev
//          of infection by severity
//          - Pulls seroincid and seroprev from DisMod
// Input:   age_mid.csv
// Output:  Datasets of incid and prev split by severity (csv, draws),
//          saved to the direcotry specified in outDir
//******************************************************************************

/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 10000
  set more off
  
  run /FILEPATH/interpolate.ado
 
  tempfile appendTemp mergeTemp adjust

* PULL IN LOCATION_ID AND INCOME CATEGORY FROM BASH COMMAND *  
  local location "`1'"

  
* SET UP OUTPUT DIRECTORIES *  
   local outDir FILEPATH

* Seroprevalence of HBsAg
* SET UP LOCALS WITH MODELABLE ENTITY IDS AND AGE GROUPS *  
  local meid OBJECT

  *Generate ages, then tokenize to loop through values to replace in template
  local ages 2 3 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 34 235 238 388 389 164
  local sexes 1 2

  local years 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024

  
/******************************************************************************\
                      PULL IN DRAWS AND MAKE CALCULATIONS
\******************************************************************************/
  
  run "FILEPATH/get_draws.ado"
  
    * PULL IN DRAWS FROM DISMOD MODELS FOR HBsAG PREVALENVCE AND INCIDENCE *
      get_draws, gbd_id_type("modelable_entity_id") gbd_id(OBJECT) source("epi") location_id(`location') age_group_id(`ages') measure_id(5) status("best") release_id(OBJECT) clear
      rename draw_* prev_*
      save `mergeTemp'
    
    * Incidence, stick prev and incidence together
      get_draws, gbd_id_type("modelable_entity_id") gbd_id(OBJECT) source("epi") location_id(`location') measure_id(6) status("best") release_id(OBJECT) clear
      rename draw_* incCarrier_*
      *drop n_draws
      merge 1:1 age_group_id year_id sex_id using `mergeTemp', gen(merge56)


      * save prevalence at birth as incidence at birth 
      forvalues i = 0/999 {
        quietly {
          replace incCarrier_`i' = prev_`i' if age_group_id == 164
        }
      }

      destring age_group_id,replace
      replace age_group_id = 235 if age_group_id==33


    merge m:1 age_group_id using /FILEPATH/hepB_chronic2acute_birthprev.dta
   
    drop if _merge == 2


    gen cause_id = 402
   
  * CORRECT INCIDENCE (RESCALE INCIDENCE TO PREVALENCE WITH AGE PATTERN BASED ON PRE-VACCINATION ESTIMATES) * 
    egen incCarrierMean = rowmean(incCarrier_*)
    egen prevMean = rowmean(prev_*)

    destring location_id, replace
    destring sex_id, replace
    destring year_id, replace
    sort location_id sex_id age_group_id year_id


  * PERFORM DRAW-LEVEL CALCULATIONS TO SPLIT TYPHOID & PARATYPHOID, & CALCULATE MRs *
    forvalues i = 0/999 {
      quietly {
        generate inc_`i' = (incCarrier_`i')  / rbeta(prcarrieralpha, prcarrierbeta)  

        *multiply by the probability of incidence of SYMPTOMATIC acute infection
        generate acute_`i' = inc_`i' * rbeta(pracutealpha, pracutebeta) 

      }
    }

/******************************************************************************\
                    EXPORT FILES AND PERFORM SEQUELA SPLITS
\******************************************************************************/    
    

  	keep if (mod(year_id,5)==0 |  year_id==2020 | year_id == 2022 | year_id == 2023 | year_id == 2024) & year_id>=1990

  * EXPORT CHRONIC PREVALENCE DRAWS * 
	local years 1990 1995 2000 2005 2010 2015 2020  2022 2023 2024

  * EXPORT CHRONIC INCIDENCE AND PREVALENCE *
  rename incCarrier_* draw_*

    foreach year in `years'{
      forvalues sex = 1/2 {
        export delimited age_group_id draw_* using `outDir'/chronic/6_`location'_`year'_`sex'.csv if sex_id==`sex' & year_id==`year', replace
      }
    }
   
  drop draw_*
  
  rename prev_* draw_*

    foreach year in `years' {
      forvalues sex = 1/2 {
        export delimited age_group_id draw_* using `outDir'/chronic/5_`location'_`year'_`sex'.csv if sex_id==`sex' & year_id==`year', replace
      }
    }
  drop draw_*
  capture drop incCarrier_* prev_*
  

  * EXPORT TOTAL ACUTE INCIDENCE AND PREVALENCE *
   rename inc_* draw_*

    foreach year in `years'{
      forvalues sex = 1/2 {
        export delimited age_group_id draw_* using `outDir'/total/6_`location'_`year'_`sex'.csv if sex_id==`sex' & year_id==`year', replace
      }
    } 
  rename draw_* inc_*


  forvalues i = 0/999 {
      quietly generate draw_`i' = inc_`i' * 6 /52 
  }
  foreach year in `years'{
    forvalues sex = 1/2 {
      export delimited age_group_id draw_* using `outDir'/total/5_`location'_`year'_`sex'.csv if sex_id==`sex' & year_id==`year', replace
    }
  }
   
  drop draw_*



  * EXPORT TOTAL SYMPTOMATIC INCIDENCE AND PREVALENCE *
  rename acute_* draw_*

    foreach year in `years'{
      forvalues sex = 1/2 {
        export delimited age_group_id draw_* using `outDir'/_symp/6_`location'_`year'_`sex'.csv if sex_id==`sex' & year_id==`year', replace
      }
    } 
  rename draw_* acute_*

  forvalues i = 0/999 {
      quietly generate draw_`i' = acute_`i' * 6 /52 
  }
  foreach year in `years'{
    forvalues sex = 1/2 {
      export delimited age_group_id draw_* using `outDir'/_symp/5_`location'_`year'_`sex'.csv if sex_id==`sex' & year_id==`year', replace
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
          export delimited age_group_id draw_* using `outDir'/`state'/6_`location'_`year'_`sex'.csv if state=="`state'" & sex_id==`sex' & year_id==`year', replace
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
            export delimited age_group_id draw_* using `outDir'/`state'/5_`location'_`year'_`sex'.csv if state=="`state'" & sex_id==`sex' & year_id==`year', replace
     }
  }
}

log close
 