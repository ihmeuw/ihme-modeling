* Purpose: submit rabies estimation by location
*
* Before running:
* 1) set run_id - make sure not to overwrite old run_id
* 2) confirm decomp steps on child script
* 3) confirm and record latest codcorrect used to make estimates
* 4) look at line 32/33 in the child script and make sure to pull from intended source

  clear all
  set more off
  local FILEPATH

  * get demographic information to pass on
  run FILEPATH
  get_demographics, gbd_team(ADDRESS) gbd_round_id(ADDRESS) clear
  local locations `r(location_id)'
  local years `r(year_id)'
  local ages `r(age_group_id)'
  local currentYear = max(`=subinstr("`years'", " ", ",", .)')
  
  * 1) set run_id 
  local run_id run8_2021-02-09
  local rootDir FILEPATH
  *local rootDir FILEPATH
  
  * create directory for draws
  local outDir `rootDir'ADDRESS
  * SUBMIT BASH FILES *
  foreach location_id of local locations {
  * Submit Tasks
	sleep 1000
	}
 
*3)
* gbd 2020 run8_2021-02-09 - CODEm bested 2.02.21