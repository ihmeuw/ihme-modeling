* BOILERPLATE *
  clear all
  set maxvar 10000
  set more off

  clear all
  set maxvar 10000
  set more off
  
  if c(os) == "Unix" {
    local j 
    set 
    }
  else if c(os) == "Windows" {
    local j 
    }
	
  
run "FILEPATH"


* ENSURE THAT OUTPUT DIRECTORIES EXIST *
local states death _asymp inf_mild inf_mod inf_sev 

capture mkdir "FILEPATH"
capture mkdir "FILEPATH"

foreach state of local states {
  capture mkdir "FILEPATH"
}
    
* PULL IN CF DRAWS AND CREATE LOCATION-SPECIFIC FILES *	
get_demographics, gbd_team() clear
  
* SUBMIT BASH FILES *
foreach location in `r(location_ids)' {
    ! qsub -P proj_covariates -pe multi_slot 8 -N split_`location' "FILEPATH" "`location'" 
}
	
