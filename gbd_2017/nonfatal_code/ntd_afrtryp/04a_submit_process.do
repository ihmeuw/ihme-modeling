  
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

 * capture mkdir FILEPATH
  local rootDir FILEPATH
  *capture mkdir FILEPATH
  
  *numlist "1462/1464 10918/10921"
  numlist "1462 10918/10921"
  local meidList `r(numlist)'
 

  *foreach subdir in `meidList' temp parent {
	*capture mkdir FILEPATH
	*}

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
  

* GET nonfatal ESTIMATES *
 use FILEPATH/nonFatalDraws.dta, clear
 levelsof location_id, local(hatLocs) clean

 
local nonhatLocs: list locations - hatLocs

use FILEPATH/nfZeros.dta, clear

generate location_id = .
generate modelable_entity_id = .

foreach meid of local meidList {
	replace modelable_entity_id = `meid'
	foreach location in `nonhatLocs' {
	replace location_id = `location'
    export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using FILEPATH/`location'.csv,  replace
	}
  }

 
use FILEPATH/nonFatalDraws.dta, clear
keep location_id year_id sex_id age_group_id measure_id total_* sleep_* disf_*
drop if missing(age_group_id)
keep if inlist(year_id, `=subinstr("`years'", " ", ",", .)')


local meid_disf_g 10918 
local meid_sleep_g 10919 

local meid_disf_r 10920 
local meid_sleep_r 10921 

local count 1
tempfile appendTemp


	
foreach stub in total sleep disf {
	foreach species in r g {
		preserve
		keep location_id year_id sex_id age_group_id measure_id `stub'_`species'_*
		
		generate outcome = "`stub'"
		generate species = "`species'"
		if "`stub'"!="total" generate modelable_entity_id = `meid_`stub'_`species''
		
		rename `stub'_`species'_* draw_*
		
		if `count'>1 append using `appendTemp'
		save `appendTemp', replace
		local ++count
		restore
		}
	}
	
use `appendTemp', clear
		

local name10918 disf_g
local name10919 sleep_g

local name10920 disf_r
local name10921 sleep_r

local name1462 total
local name1463 disf
local name1464 sleep


fastcollapse draw_* if outcome=="total", by(location_id year_id sex_id age_group_id measure_id outcome) type(sum) append
replace modelable_entity_id = 1462 if missing(species) 
drop if missing(modelable_entity_id)

*1462


*Null draw values needs to be fixed!
replace draw_737=0 if missing(draw_737)

*for some reason, one draw from measure_id==5 is missing for 2017, all countries MEID 10918

replace draw_632=0 if missing(draw_632)

	foreach location of local hatLocs {
		export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using FILEPATH/`location'.csv if modelable_entity_id==10918 & location_id==`location',  replace
		}

	
	foreach location of local hatLocs {
		export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using  FILEPATH/`location'.csv if modelable_entity_id==10919 & location_id==`location',  replace
		}
	
	foreach location of local hatLocs {
		export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using FILEPATH/`location'.csv if modelable_entity_id==10920 & location_id==`location',  replace
		}
		
	foreach location of local hatLocs {
		export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using FILEPATH/`location'.csv if modelable_entity_id==10921 & location_id==`location',  replace
		}

local mark_best True
	
run FILEPATH/save_results_epi.ado

*save_results_epi, modelable_entity_id(1462)  mark_best(`mark_best') input_file_pattern({location_id}.csv) description("HAT total incidence and prevalence") input_dir(FILEPATH/1462) clear
*save_results_epi, modelable_entity_id(10918) mark_best(`mark_best') input_file_pattern({location_id}.csv) description("HAT Rash, gambiense") input_dir(FILEPATH/10918) clear
*save_results_epi, modelable_entity_id(10919) mark_best(`mark_best') input_file_pattern({location_id}.csv) description("Sleeping sickness, gambiense") input_dir(FILEPATH/10919) clear
*save_results_epi, modelable_entity_id(10920) mark_best(`mark_best') input_file_pattern({location_id}.csv) description("HAT Rash, rhodesiense") input_dir(FILEPATH/10920) clear
*save_results_epi, modelable_entity_id(10921) mark_best(`mark_best') input_file_pattern({location_id}.csv) description("Sleeping sickness, rhodesiense") input_dir(FILEPATH/10921) clear



	