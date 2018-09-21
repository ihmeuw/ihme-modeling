
/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 15000
  set more off

* PULL IN LOCATION_ID FROM BASH COMMAND *
  local location "`1'"

  local outDir 

  tempfile adjust
  save `adjust'
  
  use "FILEPATH", clear
  gen cause_id = 401

  merge m:1 location_id year_id sex_id age_group_id using `adjust', nogen keep(3)
  destring adjust, replace


* PRODUCE DEATH DRAWS *
forvalues i = 0/999 {
  quietly {

	   generate prev_`i' = invcloglog(rnormal(fixed, fixedSe) + rnormal(random1, randomSe1) + rnormal(random2, randomSe2) + rnormal(random3, randomSe3))
     replace  prev_`i' = prev_`i' - ((prev_`i' - 0.5) * 0.00002)

	   generate inc_`i' = (-1 * ln(1 - prev_`i') / ageMid) * (1 - prev_`i')
	   replace  inc_`i' = 0 if age_group_id <= 3

     replace inc_`i' = inc_`i' * adjust

	 }
  }

* EXPORT DEATH DRAWS *

*Merge on population for nonfatal estimates
tempfile temp
save `temp', replace

get_population, age_group_id(-1) location_id(`location') year_id(-1) sex_id(-1) clear

merge m:1 location_id year_id sex_id age_group_id using `temp', nogen keep(3)

* COLLAPSE TO QUINQUENNIAL ESTIMATES FOR NON-FATAL *
keep year_id age_group_id sex_id ageMid inc_* pop inc_* prev_*
drop if year_id < 1990

collapse (mean) inc_* prev_* pop, by(year_id age_group_id ageMid sex_id)

* EXPORT SEROPREVALENCE AND INCIDENCE OF INFECTION *

local years 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016

local parameter 5

foreach prefix in prev inc {
  rename `prefix'_* draw_*
   foreach year in `years'{
    foreach sex in 1 2 {
     export delimited age_group_id draw_* using "FILEPATH" if year_id==`year' & sex_id==`sex', replace
	 }
	}
   rename draw_* `prefix'_*
   local ++parameter
   }



* PRODUCE INCIDENCE DRAWS *

*probability of acute infection by age from Armstrong & Bell,2002; DOI: 10.1542/peds.109.5.839
generate prAcute   = logit(0.852 * (1 - exp(-0.01244 * ageMid^1.903)))

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


    generate _asymp6_`i'  = inc_`i'  - sympTemp
    generate inf_sev6_`i' = rbeta(`alphaSev', `betaSev') * sympTemp
    replace  inf_sev6_`i' = 0 if missing(inf_sev6_`i')
	generate inf_mod6_`i' = sympTemp - inf_sev6_`i'
	generate inf_mild6_`i' = 0

	*assert inrange(((_asymp6_`i' + inf_sev6_`i' + inf_mod6_`i') / allTemp), 0.99, 1.01)

	drop sympTemp

    foreach seq in _asymp inf_mild inf_mod inf_sev {
	  replace  `seq'6_`i' = 0 if age_group_id <= 3

    * durations specified in weeks, the 52 converts to years
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
