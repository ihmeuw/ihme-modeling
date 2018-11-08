// Purpose: Generate estimates of Hep A deaths as a rate by multiplying symptomatic
//          incidence from epi database by case fatality rate
//			     Steps:
//            - Load the CFR from a prepped file
//			      - Pull total symptomatic incid from epi
//            - Multiply CFR times incid, and export as files
// Input:   - Total symptomatic incid from nonfatal modelling
//          - Prepped CFR
// Output:  Dataset (csv) of hep A deaths (as draws)
//******************************************************************************

/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 20000
  set more off
  
  if c(os) == "Unix" {
    local j "/home/j"
    set odbcmgr unixodbc
    }
  else if c(os) == "Windows" {
    local j "J:"
    }
	
  run /FILEPATH/interpolate.ado
 
  tempfile appendTemp mergeTemp adjust age_ids

* PULL IN LOCATION_ID AND INCOME CATEGORY FROM BASH COMMAND *  
  local location "`1'"

  log using /FILEPATH/log_`location', replace
  
* SET UP OUTPUT DIRECTORIES *  
  local outDir /FILEPATH/

  *import CFR
  import delimited /FILEPATH/age_specific_CFR_a.csv, clear
  tempfile cfr
  save `cfr'
  clear

* SET UP LOCALS WITH MODELABLE ENTITY IDS AND AGE GROUPS *  
  local meid  18834
  local ages 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235

/******************************************************************************\
                      PULL IN DRAWS AND MAKE CALCULATIONS
\******************************************************************************/
  
    * PULL IN DRAWS FROM DISMOD MODELS FOR HBsAG symptomatic incidence
      interpolate, gbd_id_type("modelable_entity_id") gbd_id(`meid') source("epi") location_id(`location') age_group_id(`ages') measure_id(6) status("best") reporting_year_start(1980) reporting_year_end(2017) clear
      rename draw_* inc_*

      gen cause_id = 401
      * replace age group 95-99 (group 33) with age group 95+ (group 235)
      destring age_group_id, replace
      replace age_group_id = 235 if age_group_id==33	

      merge m:1 age_group_id cause_id using `cfr', nogen keep(3)

    forvalues i = 0/999 {
	    quietly {
			
			* multiply by case fatality to estimate mortality rate
			generate draw_`i' = inc_`i' * cfr
      }
    }
		
/******************************************************************************\
                    EXPORT FILES WITH DEATH ESTIMATES
\******************************************************************************/		

destring sex_id, replace
destring year_id, replace

*	* EXPORT DEATHS *
forvalues year = 1980/2017 {
  forvalues sex = 1/2 {
    export delimited age_group_id draw_* using `outDir'/`location'_`year'_`sex'.csv if sex_id==`sex' & year_id==`year', replace
  }
}

log close
 
