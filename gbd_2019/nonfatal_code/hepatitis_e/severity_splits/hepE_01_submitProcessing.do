// Purpose: Launch the processing script for every most detailed location_id
//          (one job per location)
//******************************************************************************

/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 10000
  set more off

 
 
  run "FILEPATH/get_location_metadata.ado"

  get_location_metadata, location_set_id(35) gbd_round_id(6) clear

  keep if most_detailed == 1

  levelsof location_id, local(locations) clean

  foreach location in `locations' {
    ! qsub -P PROJECT -l fthread=8 -l m_mem_free=16G -l archive=TRUE  -q all.q -e FILEPATH -o FILEPATH -N splitE_`location' FILEPATH "`location'" 
  }
