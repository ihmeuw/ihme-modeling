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
		global main_dir "FILEPATH"
		global out_dir = "FILEPATH"
		global function_dir "FILEPATH"

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

	qui do "FILEPATH/reshape_pop.ado"
	qui do "FILEPATH/reshape_deaths.ado"
	qui do "FILEPATH/select_registry_years.ado"

	qui do "FILEPATH/get_locations.ado"

	set seed 1234

	import delim using "FILEPATH/location_metadata_`gbd_year'.csv", clear
	sort ihme_loc_id
	gen n = _n
	egen group = cut(n), group(150) 
	qui levelsof ihme_loc_id if group == `group', local(countries) 
	
	foreach country in `countries' {
		global pop_file "FILEPATH/d01_formatted_population_`country'.dta"
		global deaths_file "FILEPATH/d01_formatted_deaths_`country'.dta"			
		global save_pop_file "FILEPATH/d02_reshaped_population_`country'.dta"
		global save_deaths_file "FILEPATH/d02_reshaped_deaths_`country'.dta"
		
		** **********************
		** Reshape population
			noi: reshape_pop, data("$pop_file") iso3("`country'") saveas("$save_pop_file")
			
		** **********************
		** Reshape deaths
		** **********************
			noi: reshape_deaths, popdata("$save_pop_file") data("$deaths_file") iso3("`country'") saveas("$save_deaths_file")
	}
	
	capture log close
	exit, clear
