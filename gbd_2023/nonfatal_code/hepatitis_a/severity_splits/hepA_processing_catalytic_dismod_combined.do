// Purpose: Transform seroincid and seroprev estimates to incid and prev
//          of infection by severity
//          - Pulls seroincid and seroprev from DisMod
// Input:   age_mid.csv
// Output:  Datasets of incid and prev split by severity (csv, draws),
//          saved to the directory specified in outDir
//******************************************************************************

/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 20000
  set more off
  
  run /FILEPATH/interpolate.ado
 
  tempfile appendTemp mergeTemp adjust age_ids

* PULL IN LOCATION_ID AND INCOME CATEGORY FROM BASH COMMAND *  
  local location "`1'"

  
* SET UP OUTPUT DIRECTORIES *  
  local outDir FILEPATH
  
* SET UP LOCALS WITH MODELABLE ENTITY IDS AND AGE GROUPS *  
  local meid  OBJECT
  local ages 2 3 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 34 235 238 388 389
  local sexes 1 2

  local years 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024

 

/******************************************************************************\
                      PULL IN DRAWS AND MAKE CALCULATIONS
\******************************************************************************/
  
    * PULL IN DRAWS FROM DISMOD MODELS FOR ANTI-HAV PREVALENVCE *
      interpolate, gbd_id_type("modelable_entity_id") gbd_id(`meid') source("epi") status(best) location_id(`location') age_group_id(`ages') measure_id(5) reporting_year_start(1990) reporting_year_end(2024) release_id(OBJECT) clear
	  rename draw_* prev_*

      gen cause_id = OBJECT
      destring age_group_id,replace
      replace age_group_id = 235 if age_group_id==33

      save `mergeTemp'

      * load table of age_group_ids and corresponding age_mids
      import delimited using "FILEPATH/age_mid.csv", clear

      * merge that table onto the main dataframe
      merge 1:m age_group_id using `mergeTemp', gen(merge56)

      forvalues i = 0/999 {
        quietly {
          generate inc_`i' = (-1 * ln(1 - prev_`i') / age_mid) * (1 - prev_`i')
        }
      }
	

/******************************************************************************\
                    EXPORT FILES AND PERFORM SEQUELA SPLITS
\******************************************************************************/		

destring location_id,replace
destring year_id,replace
destring sex_id,replace

tempfile temp
save `temp', replace

run FILEPATH/get_population.ado
get_population, age_group_id(-1) location_id(`location') year_id(-1) sex_id(-1) release_id(OBJECT) clear

merge m:1 location_id year_id sex_id age_group_id using `temp', nogen keep(3)

* COLLAPSE TO QUINQUENNIAL ESTIMATES FOR NON-FATAL *
keep year_id age_group_id sex_id age_mid inc_* pop inc_* prev_*

keep if (mod(year_id,5)==0 |  year_id==2020 | year_id == 2022 | year_id == 2023 | year_id == 2024) & year_id>=1990

collapse (mean) inc_* prev_* pop, by(year_id age_group_id age_mid sex_id)


* PRODUCE INCIDENCE DRAWS *
forvalues i = 0/999 {
  quietly {

    *replace inc_`i' = 0 if age_group_id <= 3
    replace inc_`i' = 0 if missing(inc_`i')

    *generate prev by multiplying incid by 4 week duration
    generate t5_`i' = inc_`i' * 4 / 52

    generate t6_`i' = inc_`i'

  }
}

* EXPORT SEQUELA DRAWS - TOTAL ACUTE INCIDENCE AND PREV *  
 local years 1990 1995 2000 2005 2010 2015 2020  2022 2023 2024

foreach parameter in 5 6 {

  rename t`parameter'_* draw_*
  foreach year in `years' {
    foreach sex in 1 2 {
    export delimited age_group_id draw_* using `outDir'/total/`parameter'_`location'_`year'_`sex'.csv if year_id==`year' & sex_id==`sex', replace 
    }
  }

  rename draw_* t`parameter'_*
}



*probability of acute infection by age from Armstrong & Bell,2002; DOI: 10.1542/peds.109.5.839
generate prAcute   = logit(0.852 * (1 - exp(-0.01244 * age_mid^1.903)))

* standard error back calculated from Armstrong & Bell's CI's for prAcute in 10-17 year olds and is in logit space
generate prAcuteSe = .25

local prSev    = 0.005 / 0.7
local prSevSe  = `prSev' / 4
local alphaSev = `prSev' * (`prSev' - `prSev'^2 - `prSevSe'^2) / `prSevSe'^2
local betaSev  = `alphaSev' * (1 - `prSev') / `prSev'

forvalues i = 0/999 {
  quietly {

  *estimate number of symptomatic infections
  generate sympTemp = invlogit(rnormal(prAcute, prAcuteSe)) * inc_`i'

  generate _asymp6_`i'  = inc_`i' - sympTemp
  replace  _asymp6_`i' = 0 if missing(_asymp6_`i')
  generate inf_sev6_`i' = rbeta(`alphaSev', `betaSev') * sympTemp
  replace  inf_sev6_`i' = 0 if missing(inf_sev6_`i')
  generate inf_mod6_`i' = sympTemp - inf_sev6_`i'
  replace  inf_mod6_`i' = 0 if missing(inf_mod6_`i')

  drop sympTemp

  generate _symp6_`i' = inc_`i' - _asymp6_`i'

  foreach seq in _asymp inf_mod inf_sev _symp {
   
    * durations specified in weeks, the 52 converts to years
    generate `seq'5_`i' = `seq'6_`i' * 4 / 52
    }
  }
}



* EXPORT SEQUELA DRAWS *
*local years 1990 1995 2000 2005 2010 2015 2019 2020 2021 2022
	local years 1990 1995 2000 2005 2010 2015 2020  2022 2023 2024

foreach parameter in 5 6 {
 foreach state in inf_mod inf_sev _asymp _symp {
  rename `state'`parameter'_* draw_*
   foreach year in `years' {
    foreach sex in 1 2 {
     export delimited age_group_id draw_* using `outDir'/`state'/`parameter'_`location'_`year'_`sex'.csv if year_id==`year' & sex_id==`sex', replace
   }
  }
   rename draw_* `state'`parameter'_*
   }
  }



 
