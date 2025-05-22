* BOILERPLATE *
  clear all
  set maxvar 12000
  set more off
  local FILEPATH
  local FILEPATH
  
  run FILEPATH	
  run FILEPATH
  run FILEPATH
  run FILEPATH

  local cause_dir "FILEPATH"
  local run_dir FILEPATH
  local release_id ADDRESS

* BUILD LIST OF ENDEMIC LOCATIONS * 
  import delimited FILEPATH
  keep if value_endemicity == 1
  levelsof location_id, local(endemicLocations)
  
  get_demographics, gbd_team(ADDRESS) clear
  local years `r(year_id)'
  local ages `r(age_group_id)'
  local sexes `r(sex_id)'
	 
  macro dir
  get_draws, gbd_id_type(ADDRESS) measure_id(5) gbd_id(ADDRESS) release_id(`ADDRESS') location_id(`endemicLocations') age_group_id(`ages') source(ADDRESS) status(ADDRESS) clear
  rename location_id source_location_id

  save FILEPATH, replace
  export delimited FILEPATH, replace