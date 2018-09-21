
/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 11000
  set more off
  
  if c(os) == "Unix" {
    local j 
    set 
    }
  else if c(os) == "Windows" {
    local j 
    }
	
  
 
  tempfile appendTemp mergeTemp adjust age_ids

  save `adjust'
  clear

  *import delimited , clear
  *tempfile cfr
  *save `cfr'
  *clear

* PULL IN LOCATION_ID AND INCOME CATEGORY FROM BASH COMMAND *  
  local location "`1'"

  capture log close
  log using , replace
  
  
* SET UP OUTPUT DIRECTORIES *  
  local outDir 

* SET UP LOCALS WITH MODELABLE ENTITY IDS AND AGE GROUPS *  
  local meid  1659
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

/******************************************************************************\
                      PULL IN DRAWS AND MAKE CALCULATIONS
\******************************************************************************/
  
* PULL IN DRAWS FROM DISMOD MODELS FOR ANTI-HEV INCIDENCE AND PREVALENVCE *
interpolate, gbd_id_field(modelable_entity_id) gbd_id(`meid') source() location_ids(`location') age_group_ids(`ages') measure_ids(5) status(best) reporting_year_end(2016) clear
rename draw_* prev_*
save `mergeTemp'

interpolate, gbd_id_field(modelable_entity_id) gbd_id(`meid') source() location_ids(`location') age_group_ids(`ages') measure_ids(6) status(best) reporting_year_end(2016) clear
rename draw_* inc_*
merge 1:1 age_group_id year_id sex_id using `mergeTemp', gen(merge56)

gen cause_id = 404
replace age_group_id = 235 if age_group_id==33

merge m:1 location_id year_id sex_id age_group_id using `adjust', nogen keep(3)
destring adjust, replace

*merge m:1 age_group_id cause_id using `cfr', nogen keep(3)

* PERFORM DRAW-LEVEL CALCULATIONS TO CONVERT INCIDENCE AMONG SUSCEPTABLES TO POPULATION INCIDENCE AND ESTIMATE DEATHS *
forvalues i = 0/999 {
	quietly {

		  replace  inc_`i' = (inc_`i' * (1 - prev_`i'))  
    	replace inc_`i' = inc_`i' * adjust

    	*multiply by case fatality to estimate mortality rate
		  *generate draw_`i' = inc_`i' * cfr

  }
}

	
/******************************************************************************\
                             INTERPOLATE DEATHS
\******************************************************************************/			

  
/*append using `appendTemp'

foreach var of varlist draw_*{
  replace `var' = log(`var')
  bysort age_group_id sex_id: ipolate `var' year_id, gen(`var'_temp) epolate
  replace `var' = `var'_temp
  replace `var' = exp(`var')
  replace `var' = 0 if `var' == .
  drop `var'_temp
}


/******************************************************************************\
                    EXPORT FILES AND PERFORM SEQUELA SPLITS
\******************************************************************************/		

 * EXPORT DEATHS *
   forvalues year = 1980/2016 {
     forvalues sex = 1/2 {
        export delimited age_group_id draw_* using `outDir'/death/testing/`location'_`year'_`sex'.csv if sex_id==`sex' & year_id==`year', replace
  	    }
      }

*/
	*drop draw_* cfr
	*keep if mod(year_id,5)==0 | year_id==2016 & year_id>=1990 
  preserve

* PRODUCE INCIDENCE DRAWS *
*probability of acute infection by age from Armstrong & Bell,2002; DOI: 10.1542/peds.109.5.839

import delimited, clear
save `age_ids', replace
restore

merge m:1 age_group_id using `age_ids', nogen keep(3)
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

    generate _asymp6_`i'  = inc_`i'  - sympTemp
    generate inf_sev6_`i' = rbeta(`alphaSev', `betaSev') * sympTemp
    replace  inf_sev6_`i' = 0 if missing(inf_sev6_`i')
	 generate inf_mod6_`i' = sympTemp - inf_sev6_`i'
	 generate inf_mild6_`i' = 0 
	
	*assert inrange(((_asymp6_`i' + inf_sev6_`i' + inf_mod6_`i') / allTemp), 0.99, 1.01)
	*assert inrange(((inf_sev6_`i' + inf_mod6_`i') / sympTemp), 0.99, 1.01)
	
	drop sympTemp

    foreach seq in _asymp inf_mild inf_mod inf_sev {
	  replace  `seq'6_`i' = 0 if age_group_id <= 3
    *durations specified in weeks, the 52 converts to years
	  generate `seq'5_`i' = `seq'6_`i' * 4 / 52 

      }
	  }
  }



* EXPORT SEQUELA DRAWS *  

foreach parameter in 5 6 {
 foreach state in inf_mild inf_mod inf_sev _asymp {
  rename `state'`parameter'_* draw_*
   foreach year in `years' {
    foreach sex in 1 2 {
     export delimited age_group_id draw_* using "FILEPATH" if year_id==`year' & sex_id==`sex', replace 
	 } 
	}
   rename draw_* `state'`parameter'_*
   }
  }
  
  log close
 