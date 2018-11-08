  
  
  clear all
  set more off
  set maxvar 32000
  if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}

* ENSURE THAT OUTPUT DIRECTORIES EXIST * PUT YOUR DIRECTORIES HERE!

  capture mkdir FILEPATH
  local rootDir FILEPATH
  capture mkdir FILEPATH
  capture mkdir FILEPATH
  capture mkdir FILEPATH
  
  
  run "FILEPATH/get_demographics.ado" 
  get_demographics, gbd_team(ADDRESS) clear
  local locations `r(location_id)'
  local ages `r(age_group_id)'
  local years `r(year_id)'
  local firstYear = `=word("`years'", 1)'
  local currentYear = `=word("`years'", -1)'
  
  clear
  set obs `=wordcount("`ages'")'
  local i = 1
  generate age_group_id = .
  foreach age of local ages {
	replace age_group_id = `age' in `i'
	local ++i
	}
		
  expand `=`currentYear' - `firstYear' + 1'

  bysort age_group_id: gen year_id = `firstYear' + _n - 1
  
  expand 2, gen(sex_id)
  replace sex_id = sex_id + 1
  
  forvalues i = 0/999 {
	quietly generate draw_`i' = 0
	}
	
  generate cause_id = 350
  generate location_id = .
  
  save FILEPATH/zeros.dta, replace
  
* GET DEATH ESTIMATES *
 use FILEPATH/incAndDeathDraws.dta, clear
 levelsof location_id, local(hatLocs) clean
 
 local nonhatLocs: list locations - hatLocs
 
 macro dir
 
 use FILEPATH/zeros.dta, clear
 
 local i 1
  foreach location in `nonhatLocs' {
	replace location_id = `location'
	export delimited using FILEPATH/`location'.csv, replace
	di "iteration `i'"
	local ++i
	}
   
   
   
   
 use FILEPATH/incAndDeathDraws.dta, clear
 keep location_id year_id sex_id age_group_id deaths_*
 rename deaths_* draw_*
 
 capture generate cause_id = 350
 replace cause_id = 350

 foreach location of local hatLocs {
	preserve
	keep if location_id==`location'
	quietly export delimited using FILEPATH/`location'.csv,  replace
	restore
	drop if location_id == `location'
	}
 
 
 run FILEPATH/save_results_cod.ado
 save_results_cod, cause_id(350) mark_best("True") description("Custom poisson model of HAT (with e sampling)") input_dir(FILEPATH/deaths) input_file_pattern({location_id}.csv) clear

