  * do FILEPATH/ntd_chagas/03b_endemicPrevDrawPull.do
  
* BOILERPLATE *
  clear all
  set maxvar 12000
  set more off
   
    if c(os) == "Unix" {
        local ADDRESS "FILEPATH"
        set odbcmgr ADDRESS
        }
        
    else if c(os) == "Windows" {
        local ADDRESS "FILEPATH"
        }


    run FILEPATH
    run FILEPATH
    run FILEPATH
    run FILEPATH


* BUILD LIST OF ENDEMIC LOCATIONS * 
  import delimited FILEPATH    // georestrictions
  keep if value_endemicity == 1
  levelsof location_id, local(endemicLocations)
  
  get_demographics, gbd_team(epi) clear
  local years `r(year_id)'
  local ages `r(age_group_id)'
  local sexes `r(sex_id)'
    
  macro dir

  * change get_draws call to new
  get_draws, gbd_id_type(modelable_entity_id) measure_id(5) gbd_id(ADDRESS) decomp_step(iterative) location_id(`endemicLocations') age_group_id(`ages') source(epi) status(best) clear

  rename location_id source_location_id

  save FILEPATH, replace
  export delimited FILEPATH, replace
