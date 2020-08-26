// Purpose: Launch the processing script for every most detailed location_id
//          (one job per location)
//******************************************************************************

* BOILERPLATE *
  clear all
  set maxvar 10000
  set more off

  run "FILEPATH/get_location_metadata.ado"

  get_location_metadata, location_set_id(35) gbd_round_id(6) clear

  keep if most_detailed == 1

  levelsof location_id, local(locations) clean

  foreach location in `locations' {
     ! qsub -P PROJECT -l fthread=2 -l m_mem_free=4G -l h_rt=06:00:00  -l archive=TRUE -q all.q  -e FILEPATH -o FILEPATH -N splitC_v4_`location' FILEPATH "`location'" 
  }
