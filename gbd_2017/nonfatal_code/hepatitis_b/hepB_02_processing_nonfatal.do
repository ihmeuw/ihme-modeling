// Purpose: Pull incidence of carrier state from DisMod, and convert to incidence
//			of acute hepatitis.
//			Steps:
//			- Pull seroincid and seroprev from DisMod
//			- Pull in the chronic2acute dataset to add the age-specific probability
//				of being a carrier given that you were infected, and the probability
//				that hepatitis is symptomatic
//			- Perform splits for other severities as well
// Input:   hepB_chronic2acute_birthprev.dta
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

  log using /FILEPATH/log_`location', replace
  
* SET UP OUTPUT DIRECTORIES *  
  local outDir /FILEPATH

* Seroprevalence of HBsAg
* SET UP LOCALS WITH MODELABLE ENTITY IDS AND AGE GROUPS *  
  local meid 18673
  local ages 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 164 235
  local sexes 1 2

  
/******************************************************************************\
                      PULL IN DRAWS AND MAKE CALCULATIONS
\******************************************************************************/
  run /FILEPATH/get_draws.ado
  
    * PULL IN DRAWS FROM DISMOD MODELS FOR HBsAG PREVALENVCE AND INCIDENCE *
      get_draws, gbd_id_type("modelable_entity_id") gbd_id(`meid') source("epi") location_id(`location') age_group_id(`ages') measure_id(5) status("best") clear
      rename draw_* prev_*
      save `mergeTemp'
	  
      get_draws, gbd_id_type("modelable_entity_id") gbd_id(`meid') source("epi") location_id(`location') age_group_id(`ages') measure_id(6) status("best") clear
      rename draw_* incCarrier_*

      merge 1:1 age_group_id year_id sex_id using `mergeTemp', gen(merge56)

      * replace age group 95-99 (group 33) with age group 95+ (group 235)
      destring age_group_id,replace
      replace age_group_id = 235 if age_group_id==33

	  merge m:1 age_group_id using /FILEPATH/hepB_chronic2acute_birthprev.dta
    drop if _merge == 2

	  gen cause_id = 402

	  egen incCarrierMean = rowmean(incCarrier_*)
	  egen prevMean = rowmean(prev_*)

    destring location_id, replace
    destring sex_id, replace
    destring year_id, replace
    sort location_id sex_id age_group_id year_id

	* PERFORM DRAW-LEVEL CALCULATIONS TO SPLIT TYPHOID & PARATYPHOID, & CALCULATE MRs *
    forvalues i = 0/999 {
	    quietly {
		    
		    generate inc_`i' = (incCarrier_`i')  / rbeta(prCarrierAlpha, prCarrierBeta)  
    	 
		    generate acute_`i' = inc_`i' * rbeta(prAcuteAlpha, prAcuteBeta) 

      }
    }

		
/******************************************************************************\
                    EXPORT FILES AND PERFORM SEQUELA SPLITS
\******************************************************************************/		
	
	
	* EXPORT CHRONIC PREVALENCE DRAWS *	
  local years 1990 1995 2000 2005 2010 2017

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

    generate state = "inf_mod"  if index==1
    replace state = "inf_sev"  if index==2
    replace state = "_asymp"   if index==3
	 
    * SPLIT OUT INCIDENCE *
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
 
