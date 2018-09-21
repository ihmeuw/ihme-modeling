
/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 10000
  set more off

	
 
  tempfile appendTemp mergeTemp adjust

* PULL IN LOCATION_ID AND INCOME CATEGORY FROM BASH COMMAND *  
  local location "`1'"

  log using 
  
* SET UP OUTPUT DIRECTORIES *  


  import delimited
  save `adjust'
  clear

  *import delimited , clear
  *tempfile cfr
  *save `cfr'
  *clear

* SET UP LOCALS WITH MODELABLE ENTITY IDS AND AGE GROUPS *  
  local meid  1655
  local ages 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235
  local sexes 1 2

  local years 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016

  local inter_years 
  forvalues i = 1980/1989{
  	if !strpos("`years'", "`i'")  {
  		local inter_years = "`inter_years' `i'"
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

  *alpha and beta are derived using method of moments based on pooled 
  local cfAlpha = 3     

  *case fatatily data from Stroffolini et al (1997) & Bianco et al 2003
  local cfBeta  = 2445  
  
  local chronicAlpha = 209.8232140700943
  local chronicBeta  = 69.34291421296759

/******************************************************************************\
                      PULL IN DRAWS AND MAKE CALCULATIONS
\******************************************************************************/
  
    * PULL IN DRAWS FROM DISMOD MODELS FOR HBsAG INCIDENCE AND PREVALENVCE *
      interpolate, gbd_id_field(modelable_entity_id) gbd_id(`meid') location_ids(`location') age_group_ids(`ages') measure_ids(5) source(dismod) status(best) reporting_year_end(2016) clear
      rename draw_* prev_*
      save `mergeTemp'
	  
      interpolate, gbd_id_field(modelable_entity_id) gbd_id(`meid') location_ids(`location') age_group_ids(`ages') measure_ids(6) source(dismod) status(best) reporting_year_end(2016) clear
      rename draw_* inc_*
      merge 1:1 age_group_id year_id sex_id using `mergeTemp', gen(merge56)

      gen cause_id = 403
      replace age_group_id = 235 if age_group_id==33		
      merge m:1 location_id year_id sex_id age_group_id using `adjust', nogen keep(3)
      destring adjust, replace

      *merge m:1 age_group_id cause_id using `cfr', nogen keep(3)

	
    forvalues i = 0/999 {
	    quietly {
			local cfTemp = rbeta(`cfAlpha', `cfBeta') 
			local chronicTemp = rbeta(`chronicAlpha', `chronicBeta') 
		
			replace  inc_`i' = inc_`i'*adjust
			replace  inc_`i' = (inc_`i' * (1 - prev_`i')) 
		
			generate prevChronic_`i' = prev_`i' * `chronicTemp'
			generate incChronic_`i'  = inc_`i' * `chronicTemp'
			
			* multiply by case fatality to estimate mortality rate
			*generate draw_`i' = inc_`i' * cfr
        }
    }
		
	drop prev_*

	*save , replace	
	
/******************************************************************************\
                             INTERPOLATE DEATHS
\******************************************************************************/			

*append using `appendTemp'
	
*foreach var of varlist draw_*{
*  replace `var' = log(`var')
*  bysort age_group_id sex_id: ipolate `var' year_id, gen(`var'_temp) epolate
*  replace `var' = `var'_temp
*  replace `var' = exp(`var')
*  replace `var' = 0 if `var' == .
*  drop `var'_temp
*}
		
/******************************************************************************\
                    EXPORT FILES AND PERFORM SEQUELA SPLITS
\******************************************************************************/		


*	* EXPORT DEATHS *
*	forvalues year = 1980/2016 {
*     forvalues sex = 1/2 {
*        	export delimited age_group_id draw_* using `outDir'/death/testing/`location'_`year'_`sex'.csv if sex_id==`sex' & year_id==`year', replace
*  	    }
*      }

	*drop draw_*

	*keep if mod(year_id,5)==0 | year_id==2016 & year_id>=1990
	
	save "FILEPATH", replace
	
	* EXPORT CHRONIC PREVALENCE DRAWS *	
rename incChronic_* draw_*
foreach year in `years' {
    forvalues sex = 1/2 {
        export delimited age_group_id draw_* using "FILEPATH" if sex_id==`sex' & year_id==`year', replace
	}
}

	drop draw_*
	
	rename prevChronic_* draw_*
  	foreach year in `years' {
      forvalues sex = 1/2 {
        export delimited age_group_id draw_* using "FILEPATH" if sex_id==`sex' & year_id==`year', replace
	    }
	  }
	drop draw_*
	
	
	* SET UP SEQUELA SPLIT *
    expand 3
    bysort age_group_id year_id sex_id: generate index = _n

    generate state = "inf_mod" if index==1
    replace  state = "inf_sev" if index==2
    replace  state = "_asymp"  if index==3
	 
    local inf_mod  0.24 .06
    local inf_sev  0.01 .0025
    local _asymp   0.75 .1875

    foreach state in inf_mod inf_sev _asymp {
      gettoken mu sigma: `state'
	  local `state'Alpha = `mu' * (`mu' - `mu'^2 - `sigma'^2) / `sigma'^2 
      local `state'Beta  = ``state'Alpha' * (1 - `mu') / `mu' 
	  }
	 
    * SPLIT OUT INCIDENCE *	 
    forvalues i = 0 /999 {
	
	  local correction = 0
	  foreach state in inf_mod inf_sev _asymp {
	    local `state'Pr = rbeta(``state'Alpha', ``state'Beta')
		local correction = `correction' + ``state'Pr'
		}
	  
	  foreach state in inf_mod inf_sev _asymp {
	    local `state'Pr = ``state'Pr' / `correction'
		quietly replace inc_`i' = inc_`i' * ``state'Pr'  if state=="`state'"
	    }

	  }
	  
	  rename inc_* draw_*
	 
      * EXPORT SEQUELA INCIDENCE DRAWS *
	  foreach state in inf_mod inf_sev _asymp {
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
	  foreach state in inf_mod inf_sev _asymp {
		foreach year in `years' {
        forvalues sex = 1/2 {
          export delimited age_group_id draw_* using "FILEPATH" if state=="`state'" & sex_id==`sex' & year_id==`year', replace
		  }
		 }
		}
  
  log close
 