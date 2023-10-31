* do FILEPATH/01d_submit_split_data_builder.do

* BOILERPLATE *
  clear all
  set maxvar 10000
  set more off
  
  if c(os)=="Unix" {
	local j = FILEPATH
	local k FILEPATH

	set odbcmgr unixodbc
	}
  else {
	local j = FILEPATH
	local k FILEPATH
	}

  adopath + FILEPATH
  run FILEPATH/get_location_metadata.ado
  run FILEPATH/get_cod_data.ado
  run FILEPATH/create_connection_string.ado
 * run FILEPATH/create_connection_string.ado
	
  create_connection_string, database(shared)
  local shared = r(conn_string)
  
  tempfile master
  

  
* PULL LOCATION METADATA *  
  get_location_metadata, location_set_id(35) clear
  keep location_id is_estimate path_to_top_parent *region* location_name location_type ihme_loc_id
  keep if location_type=="admin0" | is_estimate==1
   
  split path_to_top_parent, gen(path) parse(,) destring
  rename path4 country_id
  drop path*
 
  save `master'
 


* BRING IN INCOME DATA *  
  tempfile income1 income2
  odbc load, exec("SELECT location_id, location_metadata_value AS income, location_metadata_version_id FROM location_metadata_history WHERE location_metadata_type_id = 12") `shared' clear
  quietly sum location_metadata_version_id
  keep if location_metadata_version_id==`r(max)'
  save `income1'
  
  rename location_id country_id
  save `income2'
  
  use `master' 
  merge 1:1 location_id using `income1', keep(1 3) nogenerate
  merge m:1 country_id using `income2',  update nogenerate
  replace income = "Upper middle income" if location_id==8     
  replace income = "Upper middle income" if inlist(location_id, 369, 374, 413)   
  replace income = "High income, nonOECD" if location_id==320     


  generate incomeCat = 1 if strmatch(income, "High*")==1
  replace  incomeCat = 2 if strmatch(income, "Upper *")==1
  replace  incomeCat = 3 if strmatch(income, "Low*")==1
  
  keep location_id incomeCat *region* is_estimate
  drop if missing(location_id)
  duplicates drop
	
  
  saveold FILEPATH.dta, version(13) replace
  
/*  
  
* CREATE INDIA SUBNATIONAL SPLIT FILE *  
  *get_location_metadata, location_set_id(35) clear

  use `master'
  levelsof location_id if strmatch(ihme_loc_id, "IND_*") & is_estimate==1, local(indSubs) clean 

  get_cod_data, cause_id(318) location_id(`indSubs') clear
  keep if cod_source_label=="India_SRS_states_report" & !inlist(age_group_id, 22, 27)

  collapse (sum) pop deaths, by(location_id location_name)

  gen rate = deaths / pop

  egen prPop = pc(pop), prop
  egen meanRate = total(rate * prPop)

  collapse (mean) rate meanRate , by(location_id location_name)
  gen rr = rate / meanRate

  sort location_id  
  keep location_id rr

  save FILEPATH.dta, replace
  
  
  
  
  
  
  
    levelsof location_id if strmatch(ihme_loc_id, "IDN_*") & is_estimate==1, local(indSubs) clean 
