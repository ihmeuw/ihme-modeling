 
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
  
  numlist "1462/1464 10918/10921"
  local meidList `r(numlist)'
  
  foreach subdir in `meidList' temp parent {
	capture mkdir `rootDir'/`subdir'
	}

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
		
  expand `=wordcount("`years'")'
  bysort age_group_id: gen year_id = 1985 + (_n * 5)
  replace year_id = 2016 if year_id==2015
  
  expand 2, gen(sex_id)
  replace sex_id = sex_id + 1
  
  expand 2, gen(measure_id)
  replace measure_id = measure_id + 5
  
  forvalues i = 0/999 {
	quietly generate draw_`i' = 0
	}

  save `rootDir'/nfZeros.dta, replace
  
  
* GET DEATH ESTIMATES *
 use FILEPATH/nonFatalDraws.dta, clear
 levelsof location_id, local(hatLocs) clean

 
 local nonhatLocs: list locations - hatLocs

 use `rootDir'/nfZeros.dta, clear

 generate location_id = .
 generate modelable_entity_id = .

foreach meid of local meidList {
  replace modelable_entity_id = `meid'
  foreach location in `nonhatLocs' {
	replace location_id = `location'
    export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using `rootDir'/`meid'/`location'.csv,  replace
	}
  }

 
use FILEPATH/nonFatalDraws.dta, clear
keep location_id year_id sex_id age_group_id measure_id total_* sleep_* disf_*
drop if missing(age_group_id)
keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2016)



local meid_disf_g 10918 
local meid_sleep_g 10919 

local meid_disf_r 10920 
local meid_sleep_r 10921 

local count 1
tempfile appendTemp

	
foreach stub in total sleep disf {
	foreach species in g r {
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


foreach meid in 10918 10919 10920 10921 {
	foreach location of local hatLocs {
		export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using `rootDir'/`meid'/`location'.csv if modelable_entity_id==`meid' & location_id==`location',  replace
		}
	}

run $prefix/temp/central_comp/libraries/current/save_results.do
save_results, modelable_entity_id(10918) mark_best(no) file_pattern({location_id}.csv) description("HAT Rash, gambiense") in_dir(`rootDir'/10918) env("prod")
save_results, modelable_entity_id(10919) mark_best(no) file_pattern({location_id}.csv) description("Sleeping sickness, gambiense") in_dir(`rootDir'/10919) env("prod")
save_results, modelable_entity_id(10920) mark_best(no) file_pattern({location_id}.csv) description("HAT Rash, rhodesiense") in_dir(`rootDir'/10920) env("prod")
save_results, modelable_entity_id(10921) mark_best(no) file_pattern({location_id}.csv) description("Sleeping sickness, rhodesiense") in_dir(`rootDir'/10921) env("prod")



	