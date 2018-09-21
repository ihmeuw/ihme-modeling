
/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 10000
  set more off

 
  tempfile appendTemp mergeTemp adjust

  import delimited 
  save `adjust', replace
  clear

  clear

* PULL IN LOCATION_ID AND INCOME CATEGORY FROM BASH COMMAND *  
  local location "`1'"

  log using 
  
* SET UP OUTPUT DIRECTORIES *  
  local outDir 

* SET UP LOCALS WITH MODELABLE ENTITY IDS AND AGE GROUPS *  
  local meid 1651

  *Generate ages, then tokenize to loop through values to replace in template
  local ages 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235
  local sexes 1 2

  local years 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016

  local inter_years 
  forvalues i = 1980/1989{
  	if !strpos("`years'", "`i'") {
  		local inter_years "`inter_years' `i'"
  	}
  }
  
* CREATE EMPTY ROWS FOR INTERPOLATION *

local o = 1
gen age_group_id = .
gen year_id = .
gen sex_id = .

foreach i in `inter_years'{
	foreach j in `ages'{
		foreach k in `sexes'{
			set obs `o'
			replace year_id = `i' in `o'
			replace age_group_id = `j' in `o'
			replace sex_id = `k' in `o'
			local o = `o'+1
		}
	}
}
  
save `appendTemp'
 
 
* DEFINE LOCALS WITH ALPHA AND BETA FOR CASE FATALITY *
  *local cfAlpha = 40 + 18       
  // alpha and beta are derived from
  *local cfBeta  = 7907 + 4257   
  // case fatatily data from Stroffolini et al (1997) & Bianco et al 2003
  
/******************************************************************************\
                      PULL IN DRAWS AND MAKE CALCULATIONS
\******************************************************************************/
  
    * PULL IN DRAWS FROM DISMOD MODELS FOR HBsAG INCIDENCE AND PREVALENVCE *
      interpolate, gbd_id_field(modelable_entity_id) gbd_id(`meid') source(dismod) location_ids(`location') age_group_ids(`ages') measure_ids(5) status(best) reporting_year_end(2016) clear
      rename draw_* prev_*
      save `mergeTemp'
	  
      interpolate, gbd_id_field(modelable_entity_id) gbd_id(`meid') source(dismod) location_ids(`location') age_group_ids(`ages') measure_ids(6) status(best) reporting_year_end(2016) clear
      rename draw_* incCarrier_*
      merge 1:1 age_group_id year_id sex_id using `mergeTemp', gen(merge56)

      replace age_group_id = 235 if age_group_id==33
	  merge m:1 age_group_id using 

	  gen cause_id = 402

      merge m:1 location_id year_id sex_id age_group_id using `adjust', nogen keep(3)
      destring adjust, replace

      *merge m:1 age_group_id cause_id using `cfr', nogen keep(3)
	 
	* CORRECT INCIDENCE (RESCALE INCIDENCE TO PREVALENCE WITH AGE PATTERN BASED ON PRE-VACCINATION ESTIAMTES) * 
	  egen incCarrierMean = rowmean(incCarrier_*)
	  egen prevMean = rowmean(prev_*)

      sort location_id sex_id age_group_id year_id

      by location_id sex_id age_group_id (year_id): gen inc1995   = incCarrierMean[2] if year_id>1995
      by location_id sex_id age_group_id (year_id): gen prevRatio = prevMean / prevMean[2] if year_id>1995

	* PERFORM DRAW-LEVEL CALCULATIONS & CALCULATE MRs *
      forvalues i = 0/999 {
	    quietly {
		by location_id sex_id age_group_id (year_id): replace incCarrier_`i' = inc1995 * prevRatio * incCarrier_`i' / incCarrierMean if year_id>1995
		
		*divide incidence by probability of becoming a carrier to convert incidence of carrier state to total incidence
		generate inc_`i' = (incCarrier_`i' * (1 - prev_`i'))  / rbeta(prCarrierAlpha, prCarrierBeta)  
    	replace inc_`i' = inc_`i' * adjust

		*multiply by the probability of having symptomatic acute illness to incidence of symptomatic acute infection
		generate acute_`i' = inc_`i' * rbeta(prAcuteAlpha, prAcuteBeta) 


        }
    }

/******************************************************************************\
                             Extrapolate deaths
\******************************************************************************/			

*append using `appendTemp'
	
*foreach var of varlist draw_*{
  
*  replace `var' = log(`var')
*  mixed `var' year_id || sex_id: || age_group_id: 
*  predict `var'_temp, fitted
*  replace `var' = `var'_temp if `var' == .
*  replace `var' = exp(`var')

*  drop `var'_temp
*}
		
/******************************************************************************\
                    EXPORT FILES AND PERFORM SEQUELA SPLITS
\******************************************************************************/		
		
	* EXPORT DEATHS *
*	forvalues year = 1980/2016 {
*     forvalues sex = 1/2 {
*        export delimited age_group_id draw_* using `outDir'/death/testing/`location'_`year'_`sex'.csv if sex_id==`sex' & year_id==`year', replace
*  	    }
*      }

	*drop draw_*
	*keep if mod(year_id,5)==0 | year_id==2016 & year_id>=1990
	
	
	* EXPORT CHRONIC PREVALENCE DRAWS *	
    rename incCarrier_* draw_*

  	foreach year in `years'{
      forvalues sex = 1/2 {
        export delimited age_group_id draw_* using "FILEPATH" if sex_id==`sex' & year_id==`year', replace
	    }
	  }
	drop draw_*
	
	rename prev_* draw_*

  	foreach year in `years' {
      forvalues sex = 1/2 {
        export delimited age_group_id draw_* using "FILEPATH" if sex_id==`sex' & year_id==`year', replace
	    }
	  }
	drop draw_*

	
	capture drop incCarrier_* prev_*
	
	* SET UP SEQUELA SPLIT *
    expand 4
    bysort age_group_id year_id sex_id: generate index = _n

    generate state = "inf_mild" if index==1
    replace  state = "inf_mod"  if index==2
    replace  state = "inf_sev"  if index==3
    replace  state = "_asymp" if index==4
	 
	 
	 
    * SPLIT OUT INCIDENCE *	 
    forvalues i = 0 /999 {
      quietly {
	    local prSev = rbeta(7, 19)  // This pull the proportion of acute cases that are severe from a beta distribution, based on numbers from McMahon et al
	    generate draw_`i' = acute_`i' * `prSev'  if state=="inf_sev" | state=="inf_mod"
	    replace  draw_`i' = acute_`i' - draw_`i' if state=="inf_mod"
	    replace  draw_`i' = inc_`i' - acute_`i'  if state=="_asymp"
	    replace  draw_`i' = 0 if state=="inf_mild"
	    }
	  }
	  
	  drop acute_* inc_*
	  
	 
      * EXPORT SEQUELA INCIDENCE DRAWS *
	  foreach state in inf_mild inf_mod inf_sev _asymp {
		foreach year in `years' {
        	forvalues sex = 1/2 {
          	export delimited age_group_id draw_* using "FILEPATH" if state=="`state'" & sex_id==`sex' & year_id==`year', replace
		  }
		}
	}
			
			
	  * CALCULATE PREVALENCE *	
		forvalues i = 0/999 {
	   	  quietly replace draw_`i' = draw_`i' * 6 /52 // apply six-weeks duration
		  }
			
			
	  * EXPORT SEQUELA PREVALENCE DRAWS *	
	  foreach state in inf_mild inf_mod inf_sev _asymp {
		foreach year in `years' {
        	forvalues sex = 1/2 {
          	export delimited age_group_id draw_* using "FILEPATH" if state=="`state'" & sex_id==`sex' & year_id==`year', replace
		 }
	}
}

log close
 