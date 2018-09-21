

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
  adopath + FILEPATH/stata

  tempfile asfr pop births neoqx
 

* PULL AGE-SPECIFIC FERTILITY RATES (ASFR) FROM COVARIATES DATABASE *
  get_covariate_estimates, covariate_name_short("ASFR") clear
  keep if year_id>=1980 & sex_id==2
  keep location* age_* year_id mean_value
  rename mean_value asfr
  save `asfr'
  
  levelsof year_id, local(years) clean
  levelsof age_group_id, local(ages) clean
 
* GET BIRTH DATA * 
  use FILEPATH/births_gbd2016.dta, clear
  drop if strmatch(ihme_loc_id, "S?") | strmatch(ihme_loc_id, "R?") | strmatch(ihme_loc_id, "R1?") | strmatch(ihme_loc_id, "R2?") | ihme_loc_id=="G" | year<1980
  levelsof location_id, clean local(locations)
  save `births'
  
  * Create a file of proportion of births that are male & female for use in splitting out zika-related births for congenital outcomes
  drop if sex_id==3
  rename year year_id
  
  bysort year_id location_id: egen prBirthsBySex = pc(births), prop
  
  keep year_id location_id sex_id prBirthsBySex 
  save FILEPATH/prBirthsBySex.dta, replace
  
* GET POPULATION DATA *
  
  get_population, location_id("`locations'") sex_id("2") age_group_id("`ages'") year_id("`years'") clear
  drop process_version_map_id
  save `pop'


  
  
* GET NEONATAL QX & BIRTH RATIO DATA * 
  use "FILEPATH/estimated_enn-lnn-pnn-ch-u5_noshocks.dta", clear 
  replace year = floor(year)
  keep location_name ihme_loc_id sex year q_nn_med
  
  merge 1:1 ihme_loc_id year sex using FILEPATH/births_gbd2016.dta, keep(3) nogenerate
  
  drop sex_id  
  reshape wide q_nn_med births, i(location_* ihme_loc_id year) j(sex) str
  
  generate birthRatio = birthsmale / birthsfemale
  replace  birthRatio = 1.05 if missing(birthRatio)
  
  drop births* *both source
  rename year year_id

  
* MERGE NEONATAL QX + BIRTH RATIO DATA WITH ASFR DATA *  
  merge 1:m location_id year_id using `asfr', keep(3) nogenerate

  keep if inrange(year, 1980, 2015)

 

* GENERATE PROPORTION PREGNANT VARIABLE *  
  generate qnn = ((q_nn_medmale * birthRatio) + q_nn_medfemale) / (1 + birthRatio)

  generate prPreg = asfr * (46/52) * (1 + qnn)
  
  keep  ihme_loc_id location_id year_id age_group_id asfr prPreg
  order location_id year_id age_group_id asfr prPreg
  sort  location_id year_id age_group_id
  
  merge 1:1 location_id year_id age_group_id using `pop', assert(2 3) keep(3) nogenerate
  
  generate nPreg = prPreg * population
  
  save "FILEPATH/prPreg.dta", replace
  
