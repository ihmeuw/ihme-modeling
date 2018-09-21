  
  
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

* ENSURE THAT OUTPUT DIRECTORIES EXIST

  capture mkdir FILEPATH
  local rootDir FILEPATH
  capture mkdir `rootDir'
  capture mkdir `rootDir'/deaths
  capture mkdir `rootDir'/temp
  
  
  run "FILEPATH/get_demographics.ado" 
  get_demographics, gbd_team(cod) clear
  local locations `r(location_ids)'
  local ages `r(age_group_ids)'
  local years `r(year_ids)'
  
  clear
  set obs `=wordcount("`ages'")'
  local i = 1
  generate age_group_id = .
  foreach age of local ages {
	replace age_group_id = `age' in `i'
	local ++i
	}
		
  expand 37
  bysort age_group_id: gen year_id = 1979 + _n
  
  expand 2, gen(sex_id)
  replace sex_id = sex_id + 1
  
  forvalues i = 0/999 {
	quietly generate draw_`i' = 0
	}
	
  generate cause_id = 350
  generate location_id = .
  
  save `rootDir'/zeros.dta, replace
  
* GET DEATH ESTIMATES *
 use FILEPATH/incAndDeathDraws.dta, clear
 levelsof location_id, local(hatLocs) clean
 
 local nonhatLocs: list locations - hatLocs
 
 macro dir
 
 use `rootDir'/zeros.dta, clear
 
 local i 1
  foreach location in `nonhatLocs' {
	replace location_id = `location'
	export delimited using `rootDir'/deaths/`location'.csv, replace
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
	quietly export delimited using `rootDir'/deaths/`location'.csv,  replace
	restore
	drop if location_id == `location'
	}
 
 
 run FILEPATH/save_results.do
 save_results, cause_id(350) mark_best(yes) description("Custom poisson model of HAT (with e sampling)") in_dir(FILEPATH)
