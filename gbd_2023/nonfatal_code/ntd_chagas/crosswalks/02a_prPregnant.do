*NTDs Chagas disease
*Description: Generate proportion women pregnant

* BOILERPLATE *
  clear all
  set maxvar 10000
  set more off
  
  if c(os)=="Unix" {
	local j = "FILEPATH"
	set odbcmgr unixodbc
	}
  else {
	local j = "FILEPATH"
	}

  adopath + FILEPATH  
  adopath + FILEPATH

  tempfile asfr pop births neoqx qnn
  run "FILEPATH/get_covariate_estimates.ado"
  run "FILEPATH/get_population.ado"


* PULL AGE-SPECIFIC FERTILITY RATES (ASFR) FROM COVARIATES DATABASE *

  get_covariate_estimates, covariate_id(ADDRESS) release_id(ADDRESS)
  

  *** need to subset to just females
  keep if year_id>=1980 
  keep if sex_id==2 
  keep location* age_* year_id mean_value
  rename mean_value asfr
  save `asfr'
  
  levelsof year_id, local(years) clean
  levelsof age_group_id, local(ages) clean
  levelsof location_id, local(locations) clean
 
* GET POPULATION DATA *

  get_population, location_id("`locations'") sex_id("ADDRESS") age_group_id("`ages'") year_id("`years'") release_id("ADDRESS") clear

  save `pop'

  
* MERGE NEONATAL QX + BIRTH RATIO DATA WITH ASFR DATA *  

  get_covariate_estimates, covariate_id(ADDRESS) release_id(ADDRESS) clear

  keep location_id year_id mean_value
  rename mean_value qnn
  save `qnn'

  merge m:m location_id year_id using `asfr', keep(3) nogenerate

  keep if inrange(year, 1980, 2024)

* GENERATE PROPORTION PREGNANT VARIABLE *  
  rate prPreg = asfr * (46/52) * (1 + qnn)
 
  keep location_id year_id age_group_id asfr prPreg 
  order location_id year_id age_group_id asfr prPreg
  sort  location_id year_id age_group_id
  
  merge 1:1 location_id year_id age_group_id using `pop', assert(2 3) keep(3) nogenerate
  
  generate nPreg = prPreg * population
  replace sex_id = 2
  
  export delimited FILEPATH/pr_estimates_new_demo.csv, replace
  save FILEPATH/prPreg.dta, replace