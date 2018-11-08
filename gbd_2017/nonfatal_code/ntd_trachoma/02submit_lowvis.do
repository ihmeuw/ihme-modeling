 
 
  
 
  set more off
  clear all
  set maxvar 10000
  
    if c(os) == "Unix" {
    local j "FILEPATH"
    set odbcmgr unixodbc
    }
  else if c(os) == "Windows" {
    local j "FILEPATH"
    }

  run "FILEPATH/fastcollapse.ado"
  run "FILEPATH/get_demographics.ado"

  
  
  capture mkdir FILEPATH
  capture mkdir FILEPATH
  capture !rm -rf FILEPATH
  capture mkdir FILEPATH
  
  
 use FILEPATH/trachomaImpairmentFullDraws2017_full.dta, replace
    
  
  *generate zero draws	
  
  

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
  replace measure_id = measure_id + 18
  
  forvalues i = 0/999 {
	quietly generate draw_`i' = 0
	}

 save FILEPATH/nfZeros.dta, replace
  

* GET nonfatal ESTIMATES *
 use FILEPATH/trachomaImpairmentFullDraws2017_full.dta, clear
 levelsof location_id, local(traLocs) clean

 
 local nontraLocs: list locations - traLocs

 use FILEPATH/nfZeros.dta, clear

 generate location_id = .
 generate modelable_entity_id = 1500


 foreach location in `nontraLocs' {
	replace location_id = `location'
	keep if measure_id==18
    export delimited location_id year_id sex_id age_group_id measure_id modelable_entity_id draw_* using FILEPATH/`location'.csv,  replace
	}

	
 
  
************* PREP DRAWS FOR EXPORT ****************************  
  use FILEPATH/trachomaImpairmentFullDraws2017_full.dta, clear
  
  generate modelable_entity_id = 1500
  generate measure_id = 18


  levelsof location_id, local(locations) clean
  foreach location of local locations {
	outsheet using FILEPATH/`location'.csv if location_id==`location', comma replace
	quietly drop if location_id==`location'
	}
	

* SAVE RESULTS * 
  run FILEPATH/save_results_epi.ado
  save_results_epi, modelable_entity_id(1500) measure_id(18) description("Proportion of low vision due to trachoma (custom mixed effects)-no restrict") input_dir("FILEPATH/gbd2017") input_file_pattern({location_id}.csv) clear 

 
