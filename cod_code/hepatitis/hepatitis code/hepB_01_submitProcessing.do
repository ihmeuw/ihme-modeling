* BOILERPLATE *
  clear all
  set maxvar 10000
  set more off
  
  run 
  

* ENSURE THAT OUTPUT DIRECTORIES EXIST *
  local states death _asymp inf_mild inf_mod inf_sev chronic

  capture mkdir 
  capture mkdir 
  foreach state of local states {
    capture mkdir 
	}
  
* LOAD FILE WITH CHRONIC - ACUTE CONVERSION DATA *
  use 

  levelsof location_id, local(locations) clean

* SUBMIT BASH FILES *
  foreach location in `locations' {
    ! qsub -P proj_covariates -pe multi_slot 8 -N split_`location' "" "`location'" 
	}
	
	