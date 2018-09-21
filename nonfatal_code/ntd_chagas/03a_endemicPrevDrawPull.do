  
* BOILERPLATE *
  clear all
  set maxvar 12000
  set more off
   
  if c(os) == "Unix" {
    local j "FILEPATH"
    set odbcmgr unixodbc
    }
  else if c(os) == "Windows" {
    local j "FILEPATH"
    }
	
  
  adopath + FILEPATH
  run FILEPATH/get_location_metadata.ado	
  run FILEPATH/get_demographics.ado
  run FILEPATH/get_draws.ado

 

 
* BUILD LIST OF ENDEMIC LOCATIONS * 
  get_location_metadata, location_set_id(8) clear
  generate endemic = (strmatch(lower(region_name), "*latin america*") | inlist(ihme_loc_id, "BLZ", "GUY", "SUR")) 
  keep location_id parent_id location_type ihme_loc_id endemic is_estimate
  keep if location_type=="admin0"
  levelsof location_id if endemic==1, local(endemicLocations) clean


  
  get_demographics, gbd_team(epi) clear
  local years `r(year_ids)'
  local ages `r(age_group_ids)'
  local sexes `r(sex_ids)'
	


  get_draws, gbd_id_field(modelable_entity_id) measure_ids(5) gbd_id(1450) location_ids(`endemicLocations') age_group_ids(`ages') source(dismod) model_version_id(164831) clear

  
  rename location_id source_location_id
  save FILEPATH/endemicPrevDraws.dta, replace
