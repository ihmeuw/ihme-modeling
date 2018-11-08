// Purpose: Generate estimates of Hep E deaths as a rate by multiplying symptomatic
//          incidence from epi database by case fatality rate
//           Steps:
//            - Load the CFR from a prepped file
//            - Pull total symptomatic incid from epi
//            - Multiply CFR times incid, and export as files
// Input:   - Total symptomatic incid from nonfatal modelling
//          - Prepped CFR
// Output:  Dataset (csv) of hep E deaths (as draws)
//******************************************************************************

/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 11000
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

  *import CFR
  import delimited /FILEPATH/age_specific_CFR_all_e.csv, clear
  tempfile cfr
  save `cfr'
  clear

* PULL IN LOCATION_ID AND INCOME CATEGORY FROM BASH COMMAND *  
  local location "`1'"

  capture log close
  log using /FILEPATH/log_`location', replace
  
* SET UP OUTPUT DIRECTORIES *  
  local outDir /FILEPATH

* SET UP LOCALS WITH MODELABLE ENTITY IDS AND AGE GROUPS *  
  local meid 18837
  local ages 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235
  local sexes 1 2
  local years 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017

/******************************************************************************\
                      PULL IN DRAWS AND MAKE CALCULATIONS
\******************************************************************************/
  
interpolate, gbd_id_type("modelable_entity_id") gbd_id(`meid') source("epi") location_id(`location') age_group_id(`ages') measure_id(6) status("best") reporting_year_start(1980) reporting_year_end(2017) clear
rename draw_* inc_*

gen cause_id = 404

* replace age group 95-99 (group 33) with age group 95+ (group 235)
destring age_group_id, replace
replace age_group_id = 235 if age_group_id==33

merge m:1 age_group_id cause_id using `cfr', nogen keep(3)

* PERFORM DRAW-LEVEL CALCULATIONS TO ESTIMATE DEATHS *
forvalues i = 0/999 {
	quietly {
		  generate draw_`i' = inc_`i' * cfr
  }
}

/*****************************************************************************
                    EXPORT FILES AND PERFORM SEQUELA SPLITS
*****************************************************************************/		

destring year_id, replace
destring sex_id, replace

 * EXPORT DEATHS *
   forvalues year = 1980/2017 {
     forvalues sex = 1/2 {
        export delimited age_group_id draw_* using `outDir'/`location'_`year'_`sex'.csv if sex_id==`sex' & year_id==`year', replace
  	    }
       }
  
log close
