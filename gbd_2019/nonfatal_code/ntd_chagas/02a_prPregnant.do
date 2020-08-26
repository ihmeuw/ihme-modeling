* do FILEPATH/ntd_chagas/02a_prPregnant.do

* BOILERPLATE *
  clear all
  set maxvar 10000
  set more off
  
if c(os) == "Unix" {
  local FILEPATH FILEPATH
  set odbcmgr ADDRESS
  }
else {
  local FILEPATH FILEPATH
  }


  adopath + FILEPATH  
  adopath + FILEPATH

  tempfile asfr pop births neoqx
  run "FILEPATH"



* PULL AGE-SPECIFIC FERTILITY RATES (ASFR) FROM COVARIATES DATABASE *
  import delimited FILEPATH  // asfr
  
  keep if year_id>=1980 
  *& sex_id==2
  keep location* age_* year_id mean_value
  rename mean_value asfr
  save `asfr'
  
  levelsof year_id, local(years) clean
  levelsof age_group_id, local(ages) clean


* GET POPULATION DATA *
  import delimited FILEPATH, clear  // pop
  save `pop'
  
  
* MERGE NEONATAL QX + BIRTH RATIO DATA WITH ASFR DATA *  
  import FILEPATH, clear  // qnn
  merge m:m location_id year_id using `asfr', keep(3) nogenerate

  keep if inrange(year, 1980, 2017)

* GENERATE PROPORTION PREGNANT VARIABLE *  
  generate prPreg = asfr * (46/52) * (1 + qnn)
  keep location_id year_id age_group_id asfr prPreg 
  order location_id year_id age_group_id asfr prPreg
  sort  location_id year_id age_group_id
  
  merge 1:1 location_id year_id age_group_id using `pop', assert(2 3) keep(3) nogenerate

  generate nPreg = prPreg * population
  replace sex_id = 2

  export delimited FILEPATH, replace
  save FILEPATH, replace
