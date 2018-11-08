********************************************************
** Author: 
** Date created: August 10, 2009
** Description:
** Formats population and deaths data.
**
**
**
** NOTE: IHME OWNS THE COPYRIGHT
********************************************************

** **********************
** Set up Stata 
** **********************

	clear all
	capture cleartmp 
	set mem 500m
	set more off

** **********************
** Filepaths 
** **********************

	if (c(os)=="Unix") { 	
		global root "/home/j"
		local group `1' 
		local version_id `2'
		local gbd_year `3'
		local user = "`c(username)'"
		local code_dir "FILEPATH"
		global function_dir "FILEPATH"
		global main_dir "FILEPATH"
		global out_dir = "FILEPATH"
	} 
	if (c(os)=="Windows") {     
        global root "J:"
		local group = "5" 
		local user = "`c(username)'"
		local code_dir "FILEPATH"
		global function_dir "FILEPATH"
		global out_dir = "FILEPATH"
	} 

** **********************
** Load functions 
** **********************		
	qui do "FILEPATH/combine_reshaped.ado"
	qui do "FILEPATH/get_locations.ado"

	set seed 1234

	import delim using "FILEPATH/location_metadata_`gbd_year'.csv", clear
	sort ihme_loc_id
	gen n = _n
	egen group = cut(n), group(150) 
	qui levelsof ihme_loc_id if group == `group', local(countries) 
	
	
** **********************
** Combine reshaped population and deaths
** **********************
	foreach country in `countries' {
		global pop_file = "FILEPATH/d02_reshaped_population_`country'.dta"
		global deaths_file = "FILEPATH/d02_reshaped_deaths_`country'.dta"
		global save_file "FILEPATH/d03_combined_population_and_deaths_`country'.dta"

		noi: combine_reshaped, popdata("$pop_file") deathsdata("$deaths_file") saveas("$save_file")
	}
	
	exit, clear

