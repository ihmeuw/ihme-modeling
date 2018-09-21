

* PREP STATA *
  clear all 
  set more off, perm
  set maxvar 10000


* ESTABLISH TEMPFILES AND APPROPRIATE DRIVE DESIGNATION FOR THE OS * 
  if c(os) == "Unix" {
    local j "FILEPATH"
    set odbcmgr unixodbc
    }
  else if c(os) == "Windows" {
    local j "FILEPATH"
    }
	 
  local inputDir  FILEPATH	 
	 
  tempfile pop bra

  adopath + FILEPATH
  
  
* LOAD THE SKELETON, FOR BRAZILIAN STATES AND ALL AGES *  
  use `inputDir'/skeleton.dta, clear
  keep if strmatch(ihme_loc_id, "BRA_*") & age_group_id==22 & year>=1980 & sex==3
  keep location_id parent_id location_name ihme_loc_id year_id sex_id population
  bysort year_id: egen mean_pop_bra = total(population)
  save `pop'
  
  levelsof location_id, local(locations) clean
  
  
* PULL THE ALL-AGE YF COD DATA FOR BRAZILIAN STATES *  
  get_cod_data, cause_id(358) location_id(`locations') age_group_id(22) clear
  bysort location_id year: gen count = _N
  drop if count==1
  
  fastcollapse deaths, by(location* year) type(sum)

  rename year year_id
  keep deaths location* year_id
  
  merge 1:1 location_id year_id  using `pop', assert(2 3) nogenerate
  
  generate endemic = !inlist(location_id, 4751, 4755, 4764, 4766, 4769, 4774)
  
  replace deaths = deaths * endemic   // zero out deaths in non-endemic states
  generate rate  = deaths / population
  

  
  
* COMPLETE COD TIME-SERIES *  
  
  mkspline yearS = year_id, cubic nknots(4)
  
  mepoisson deaths yearS* if endemic==1, exp(population) || location_id: R.year_id

  predict fixed, xb
  predict fixedSe, stdp
  predict random, remeans reses(randomSe)
  replace random = 0 if missing(random)
  quietly sum random
  local random = `r(sd)'
  quietly sum randomSe
  replace randomSe = `r(mean)' + `random'  if missing(randomSe)

  
  
* CREATE DRAWS OF PROPORTIONS *  
  forvalues i = 0 / 999 {
    quietly {
     generate braSubPr_`i' = exp(rnormal(fixed, fixedSe) + rnormal(random, randomSe))
	 replace  braSubPr_`i' = 0 if endemic==0
	 bysort year_id: egen correction = total(braSubPr_`i')
	 replace braSubPr_`i' = braSubPr_`i' / correction
	 drop correction
	 }
	}
	

  
  sort year_id location_id
  keep year_id location_id braSubPr_* mean_pop_bra

  save FILEPATH/braSubPr.dta, replace
  



