  
* BOILERPLATE *
  clear all
  set maxvar 12000
  set more off
   
  if c(os) == "Unix" {
    local j "FILENAME"
    set odbcmgr unixodbc
    }
  else if c(os) == "Windows" {
    local j "FILENAME"
    }
	
  
  adopath + FILENAME
  run FILENAME/get_location_metadata.ado	
  run FILENAME/get_demographics.ado
  run FILENAME/get_draws.ado

 

 * BUILD LIST OF ENDEMIC LOCATIONS * 
  get_location_metadata, location_set_id(35) clear
  generate endemic = (strmatch(lower(region_name), "*latin america*") | inlist(ihme_loc_id, "BLZ", "GUY", "SUR")) 
  keep location_id parent_id location_type ihme_loc_id endemic is_estimate
  keep if location_type=="admin0"
  levelsof location_id if endemic==1, local(endemicLocations) clean


  
  get_demographics, gbd_team(ADDRESS) clear
  local years `r(year_id)'
  local ages `r(age_group_id)'
  local sexes `r(sex_id)'
	
macro dir

  get_draws, gbd_id_type(modelable_entity_id) measure_id(5) gbd_id(1450) location_id(`endemicLocations') age_group_id(`ages') source(ADDRESS) status(best) clear

  
  rename location_id source_location_id
  save FILEPATH/endemicPrevDraws.dta, replace
