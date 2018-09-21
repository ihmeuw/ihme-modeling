
* BOILERPLATE *
  set more off
  clear all
  set maxvar 10000
  
    if c(os) == "Unix" {
    local j "FILEPATH"
    set odbcmgr unixodbc
    }
  else if c(os) == "Windows" {
    local j "FILEPATH"
    }

  

  capture log close
  log using FILEPATH/blindness.txt, replace

  
* PULL IN DATA *   
  use  FILEPATH/modellingData.dta, clear

  
* PREP DATA FOR MODELLING *  
  bysort nid: gen index = _n if !missing(nid)
  expand 3 if index==1, gen(newObs)
  foreach var of varlist ageMid cases*  {
    replace `var' = 0 if newObs==1
    }
  
  bysort nid newObs (ageMid): gen index2 = _n
  replace ageMid = 10 if index2==2 & newObs==1

  drop newObs index* 

  replace ageMid = 84.5 if age_group_id==21
  replace cv_ref=1 if missing(cv_ref)

  recode sex_id (1 = -1) (3 = 0) (2 = 1), gen(sexOrdered)

  foreach var of varlist sample* {
    replace `var' = 1 if missing(`var') 
    }
  
  bysort location_id: egen maxRisk = max(prAtRiskTrachoma)
  bysort location_id: egen anyNonZero = max(casesBlindness>0 & !missing(casesBlindness))
  generate endemic = (maxRisk>0 | anyNonZero>0)
  replace endemic = 0 if (strmatch(iso3, "CHN_*") & !inlist(location_id, 502, 513, 504, 520, 494, 500, 491, 495))
  replace endemic = 1 if location_id==171
  
  generate meanBlindness = casesBlindness / sampleBlindness
  generate logitMeanBlindness = logit(meanBlindness + 0.0000001)

  gen lnPar = ln(prAtRiskTrachoma+.0001)
  
  pca prAtRiskTrachoma sanitation
  predict pca1 pca2
  
  mkspline ageS = ageMid, cubic knots(0 40 100)  
 

* RUN FULL MODEL TO EXTRACT COEFFICIENT ON SEX * 
  mepoisson meanBlindness ageS* pca1 sexOrdered year_id || super_region_id: || region_id: || country_id:
  local sexBeta = _b[sexOrdered]

  
* RUN PREDICTION MODEL *  
  mepoisson meanBlindness ageS* pca1 year_id || super_region_id: || region_id: || country_id:

  
* PREDICT OUT FIXED AND RANDOM EFFECTS AND THEIR STANDARD ERRORS *  
  predict fixed, xb
  predict fixedSe, stdp
  predict random*, remeans reses(randomSe*)

  foreach var of varlist randomSe* {
    quietly summarize `var' if e(sample)
    quietly replace `var' = `r(mean)' if missing(`var')
    }

  foreach var of varlist random? {
    quietly generate `var'_temp = `var'
    quietly summarize `var'
    local `var'_mean = `r(mean)'
    local `var'_sd   = `r(sd)'
    }


	
* CREATE DRAWS *	
  forvalues i = 0/1000 {
    quietly {
      foreach var of varlist random? {
        replace `var'_temp = rnormal(``var'_mean', ``var'_sd' ) if missing(`var')
	    }
  
      generate draw_`i' = exp(rnormal(fixed, fixedSe) + (`sexBeta' * sexOrdered) + rnormal(random1_temp, randomSe1) + rnormal(random2_temp, randomSe2)+ rnormal(random3_temp, randomSe3))
	  replace  draw_`i' = 0 if age_group_id<8 | age_end<15 | endemic == 0
	  
	  count if draw_`i' > 1
	  local repeat = `r(N)'
	  while `repeat'>0 {
	    foreach var of varlist random? {
          replace `var'_temp = rnormal(``var'_mean', ``var'_sd' ) if missing(`var')
		  }
		replace  draw_`i' = exp(rnormal(fixed, fixedSe) + (`sexBeta' * sexOrdered) + rnormal(random1_temp, randomSe1) + rnormal(random2_temp, randomSe2)+ rnormal(random3_temp, randomSe3)) if draw_`i'> 1
	    count if draw_`i' > 1
		local repeat = `r(N)'
		}
      replace  draw_`i' = draw_`i' / 2 if age_group_id==8
	  }
    di "." _continue
    }



* COLLAPSE INTO 5-YEAR WINDOWS *	
  keep if !missing(age_group_id) & is_estimate==1 & inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2016)

  fastcollapse draw_*, by(location_id year_id age_group_id sex_id) type(mean)
  
  
* PREP DRAWS FOR EXPORT *  
  generate modelable_entity_id = 1503
  generate measure_id = 18

  capture mkdir FILEPATH
  capture mkdir FILEPATH
  capture mkdir FILEPATH

  outsheet using FILEPATH/full.csv, comma replace


* SAVE RESULTS * 
  local inpath = "FILEPATH/full.csv"
  local outpath = "FILEPATH/full.h5"
  !FILEPATH/to_hdf.py "`inpath'" "`outpath'"

  run FILEPATH/save_results.do
  save_results, modelable_entity_id(1503) description("Proportion of blindess due to trachoma (custom mixed effects)") in_dir("FILEPATH") file_pattern("full.h5") h5_tablename(draws)  env("prod")


  
  log close
