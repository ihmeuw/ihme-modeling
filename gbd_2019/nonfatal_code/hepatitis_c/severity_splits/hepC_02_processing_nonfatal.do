// Purpose: Transform seroincid and seroprev estimates to incid and prev
//          of infection by severity
//          - Pulls seroincid and seroprev from DisMod
//******************************************************************************

/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 10000
  set more off
  
  if c(os) == "Unix" {
    local j "/home/j"
    set odbcmgr unixodbc
    }
  else if c(os) == "Windows" {
    local j "J:"
    }
	
  run FILEPATH/interpolate.ado
 
  tempfile appendTemp mergeTemp adjust

* PULL IN LOCATION_ID AND INCOME CATEGORY FROM BASH COMMAND *  
  local location "`1'"
  
* SET UP OUTPUT DIRECTORIES *  
  local outDir FILEPATH


* SET UP LOCALS WITH MODELABLE ENTITY IDS AND AGE GROUPS * 
 *seroprev ME = 1655
 *loop 2 ME = 18672 
  local meid  OBJECT
  local ages 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235
  local sexes 1 2

  local years 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 
 
 
* DEFINE LOCALS WITH ALPHA AND BETA FOR CASE FATALITY *

  *alpha and beta are derived using method of moments based on pooled 
  local cfAlpha = 3     

  *case fatality data from Stroffolini et al (1997) & Bianco et al 2003
  local cfBeta  = 2445  
  
  local chronicAlpha = 209.8232140700943
  local chronicBeta  = 69.34291421296759

/******************************************************************************\
                      PULL IN DRAWS AND MAKE CALCULATIONS
\******************************************************************************/
  
    * PULL IN DRAWS FROM DISMOD MODELS FOR anti-HCV INCIDENCE AND PREVALENVCE *
      interpolate, gbd_id_type("modelable_entity_id") gbd_id(`meid') source("epi") location_id(`location') age_group_id(`ages') measure_id(5) status("best") reporting_year_start(1990) reporting_year_end(2019) gbd_round_id(6) decomp_step("step4") clear
      rename draw_* prev_*
      save `mergeTemp'
	  
      interpolate, gbd_id_type("modelable_entity_id") gbd_id(`meid') source("epi") location_id(`location') age_group_id(`ages') measure_id(6) status("best") reporting_year_start(1990) reporting_year_end(2019) gbd_round_id(6) decomp_step("step4") clear
      rename draw_* inc_*
      *drop n_draws
      merge 1:1 age_group_id year_id sex_id using `mergeTemp', gen(merge56)

      gen cause_id = 403

 	  destring age_group_id,replace
      replace age_group_id = 235 if age_group_id==33	
    


    forvalues i = 0/999 {
	    quietly {
			local cfTemp = rbeta(`cfAlpha', `cfBeta') 
			local chronicTemp = rbeta(`chronicAlpha', `chronicBeta') 
		
			generate prevChronic_`i' = prev_`i' * `chronicTemp'
			generate incChronic_`i'  = inc_`i' * `chronicTemp'
        }
    }	
	drop prev_*

		
/******************************************************************************\
                    EXPORT FILES AND PERFORM SEQUELA SPLITS
\******************************************************************************/		

	destring location_id, replace
    destring sex_id, replace
    destring year_id, replace

	keep if (mod(year_id,5)==0 | year_id==2017 | year_id==2019) & year_id>=1990


	local years 1990 1995 2000 2005 2010 2015 2017 2019 
	

	* EXPORT ACUTE (TOTAL) INCIDENCE AND PREVALENCE *
	forvalues i = 0/999 {
  		quietly {
    		replace inc_`i' = 0 if missing(inc_`i')
  		}
	}

	rename inc_* draw_*
	foreach year in `years' {
		forvalues sex = 1/2 {
			export delimited age_group_id draw_* using FILEPATH if sex_id==`sex' & year_id==`year', replace
		}
	}
	rename draw_* inc_*

	*generate acute (total) prevalence
	forvalues i = 0/999 {
	   	quietly generate draw_`i' = inc_`i' * 6/52 
	}

	foreach year in `years' {
		forvalues sex = 1/2 {
			export delimited age_group_id draw_* using FILEPATH if sex_id==`sex' & year_id==`year', replace
		}
	}
	drop draw_*

	
	
	* SET UP SEQUELA SPLIT *
    expand 4
    bysort age_group_id year_id sex_id: generate index = _n

    generate state = "inf_mod" if index==1
    replace  state = "inf_sev" if index==2
    replace  state = "_asymp"  if index==3
    replace  state = "_symp"   if index==4
	 
	 *why is the severity proportion followed by the same value divided by 4?
    local inf_mod  0.24 .06
    local inf_sev  0.01 .0025
    local _asymp   0.75 .1875

    * gettoken takes next token from state, and stores it in mu. the rest of 
    * the string from state is stored in sigma.
    * seems like this section calculates alpha and beta from mean (mu) and
    * standard dev (sigma) based on the values stored above, and assuming a 
    * beta distribution
    foreach state in inf_mod inf_sev _asymp {
      gettoken mu sigma: `state'
	  local `state'Alpha = `mu' * (`mu' - `mu'^2 - `sigma'^2) / `sigma'^2 
      local `state'Beta  = ``state'Alpha' * (1 - `mu') / `mu' 
	  }
	 
    * SPLIT OUT INCIDENCE *	 
    forvalues i = 0/999 {

	  local correction = 0

	  foreach state in inf_mod inf_sev _asymp {
	    local `state'Pr = rbeta(``state'Alpha', ``state'Beta')
		local correction = `correction' + ``state'Pr'
		}
	  
	  foreach state in inf_mod inf_sev _asymp {
	    local `state'Pr = ``state'Pr' / `correction'
		quietly replace inc_`i' = inc_`i' * ``state'Pr'  if state=="`state'"
	    }

	  * generate symptomatic incidence as 1 - prob asymp
	  quietly replace inc_`i' = inc_`i' * (1 - `_asympPr')  if state=="_symp"
	  
	 }
	 
	  rename inc_* draw_*
	 

      * EXPORT SEQUELA INCIDENCE DRAWS *
	  foreach state in inf_mod inf_sev _asymp _symp {
		foreach year in `years' {
        forvalues sex = 1/2 {
          export delimited age_group_id draw_* using FILEPATH if state=="`state'" & sex_id==`sex' & year_id==`year', replace
		  }
		 }
		}
			
			
	  * CALCULATE PREVALENCE *	
	  * As incidence times duration, apply six-weeks duration
		forvalues i = 0/999 {
	   	  quietly replace draw_`i' = draw_`i' * 6 /52 
		  }
			
			
	  * EXPORT SEQUELA PREVALENCE DRAWS *	
	  foreach state in inf_mod inf_sev _asymp _symp {
		foreach year in `years' {
        forvalues sex = 1/2 {
          export delimited age_group_id draw_* using FILEPATH if state=="`state'" & sex_id==`sex' & year_id==`year', replace
		  }
		 }
		}
 
