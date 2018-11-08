 
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

	
  adopath + FILEPATH
  
* ENSURE THAT OUTPUT DIRECTORIES EXIST * PUT YOUR DIRECTORIES HERE!

  capture mkdir FILEPATH
  local rootDir FILEPATH
  capture mkdir `rootDir'
  
  *MEIDs "10523 19838 11647"
  numlist "10523 19838 11647"
  local meidList `r(numlist)'
 

  foreach subdir in `meidList' temp parent {
	capture mkdir FILEPATH
	}

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
  keep if inlist(year_id, `=subinstr("`years'", " ", ",", .)')
  
  expand 2, gen(sex_id)
  replace sex_id = sex_id + 1
  
  expand 2, gen(measure_id)
  replace measure_id = measure_id + 5
  
  forvalues i = 0/999 {
	quietly generate draw_`i' = 0
	}

  save FILEPATH/nfZeros.dta, replace
  


 use FILEPATH/10523.dta, clear
 keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2017)
levelsof location_id, local(gwLocs) clean

 
local nongwLocs: list locations - gwLocs

use `rootDir'/nfZeros.dta, clear

generate location_id = .
generate modelable_entity_id = .

foreach meid of local meidList {
 replace modelable_entity_id = `meid'
 foreach location in `nongwLocs' {
 	replace location_id = `location'
    export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using FILEPATH/`location'.csv,  replace
	}
 }

 
 
*Export out the endemic country draws for incidence
 *use FILEPATH/10523.dta, clear
 *levelsof location_id, local(gwLocs) clean
  *keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2017)
*foreach location in `gwLocs' {
*	replace modelable_entity_id=10523
	
*	export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using FILEPATH/`location'.csv if location_id==`location',  replace
*	}
 
 
 use FILEPATH/10523.dta, clear
 keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2017)
 levelsof location_id, local(gwLocs) clean
 
 forvalues i=0/999 {

 quietly replace draw_`i'=draw_`i'*(1/12) if measure_id==5
 }
 
 
foreach location in `gwLocs' {
		replace modelable_entity_id=19838
	export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using FILEPATH/`location'.csv if location_id==`location',  replace
	}
 
 
 *use FILEPATH/10523.dta, clear
*keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2017)
*	levelsof location_id, local(gwLocs) clean
*	forvalues i=0/999 {
 *        quietly replace draw_`i'=draw_`i'*((2/12)+(.3*(9/12))) if measure_id==5
  *       }
    
	*foreach location in `gwLocs' {
	*	replace modelable_entity_id=11647
		
	*export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using FILEPATH/`location'.csv if location_id==`location',  replace
	*}
 

 
 


	