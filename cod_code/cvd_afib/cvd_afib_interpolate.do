	// prep stata
	clear all
	set more off
	

	// Set OS flexibility 
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
		local h "~"
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
		local personal_prefix "FILEPATH"
	}
	sysdir set PLUS "FILEPATH"
	
	
// SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
  adopath + "FILEPATH"
 

// PULL IN LOCATION_ID AND INCOME CATEGORY FROM BASH COMMAND
  local age_group "`1'"
  	  
  capture log close
  log using FILEPATH/afib_interpolate_`age_group', replace
  
// 01/16/2017 - shared function updated to interpolate from 1980 to 2016; edited code
interpolate, gbd_id_field(modelable_entity_id) gbd_id(9366) age_group_ids(`age_group') status(best) source(epi) measure_ids(15) reporting_year_start(1980) reporting_year_end(2016)
drop if age_group_id==164
forvalues i = 0/999 {
	    quietly replace draw_`i' = 0 if age_group_id<11
	}
generate cause_id=500
drop modelable_entity_id measure_id

outsheet using "FILEAPTH/interpolated_`age_group'.csv", comma replace
log close
