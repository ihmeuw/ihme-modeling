* NTDs - Chagas disease 
* Description: post processing, submits child jobs for each location and meditates whether not endemic, endemic, or endemic and eradicated

* BOILERPLATE *
  clear all
  set maxvar 12000
  set more off
  local FILEPATH
  local FILEPATH

  run FILEPATH

  tempfile endemic
  local FILEPATH
	local FILEPATH
  local FILEPATH
  local release_id ADDRESS
  local FILEPATH

  
* CREATE DIRECTORIES * 
  
  foreach subDir in inputs progress {
	capture mkdir FILEPATH
	}
  capture mkdir "FILEPATH"
 
* BUILD LIST OF ENDEMIC LOCATIONS * 
  get_location_metadata, location_set_id(ADDRESS) clear
  keep if is_estimate==1 
  generate endemic =(strmatch(lower(region_name), "*latin america*") | inlist(ihme_loc_id, "BLZ", "GUY", "SUR"))
  keep location_id endemic
	
  merge 1:m location_id using FILEPATH, nogenerate

  levelsof location_id if endemic==1, local(endemicLocations) clean
  levelsof location_id if missing(draw_0) & endemic!=1, local(zeroLocations) clean
  levelsof location_id if !missing(draw_0) & endemic!=1,  local(nonZeroLocations) clean
  
  preserve
  use `FILEPATH, clear
  foreach location in `endemicLocations' `nonZeroLocations' `zeroLocations' {
    quietly save FILEPATH, replace
	di "." _continue
	}
  restore

 foreach location in `endemicLocations' {
   * Launch parallel task script:
    "FILEPATH/04c_firstSplit.do"
   sleep 100
   }

  foreach location in `nonZeroLocations' {
   preserve
   keep if location_id==`location'
   save FILEPATH, replace
   restore
   * Launch parallel task script:
    "FILEPATH/04c_firstSplit.do"

   }
   
  foreach location of local zeroLocations {
    * Launch parallel task script:
    "FILEPATH/04f_zeros.do"
  
    sleep 100
   }
  

   log close 