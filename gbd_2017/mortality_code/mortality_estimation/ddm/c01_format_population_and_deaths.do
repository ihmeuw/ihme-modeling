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
		global main_dir = "FILEPATH"
		global out_dir = "FILEPATH"
		local code_dir "FILEPATH"
		di "`code_dir'"
		global function_dir "FILEPATH"
	} 
	if (c(os)=="Windows") { 
		global root "J:"
		local group = "5" 
		local user = "`c(username)'"
		
		global out_dir = "FILEPATH"
		local code_dir "FILEPATH"
		global function_dir "FILEPATH" 
	} 


	** Bring in cited compiled deaths and population data
	global pop_file "FILEPATH/d00_compiled_population.dta"
	global deaths_file "FILEPATH/d00_compiled_deaths.dta"	

** **********************
** Load functions 
** **********************

	qui do "FILEPATH/orderednaming.ado"
	qui do "FILEPATH/onehundredyears.ado"
	qui do "FILEPATH/get_locations.ado"
	
	set seed 1234 

	import delim using "FILEPATH/location_metadata_`gbd_year'.csv", clear
	sort ihme_loc_id
	gen n = _n
	egen group = cut(n), group(150)
	qui levelsof ihme_loc_id if group == `group', local(countries) 


	foreach country in `countries' {	
		** **********************
		** Format population
		** **********************
			
			noi: orderednaming, data("$pop_file") iso3("`country'") type("pop") version("`version_id'")
			tempfile pop
			save `pop', replace

			noi: onehundredyears, data(`pop') varname("pop") iso3("`country'")

			save "$out_dir/d01_formatted_population_`country'.dta", replace

		** **********************
		** Format deaths
		** **********************

			noi: orderednaming, data("$deaths_file") iso3("`country'") type("deaths") version("`version_id'")
			tempfile deaths
			save `deaths', replace

			noi: onehundredyears, data(`deaths') varname("deaths") iso3("`country'")

			save "$out_dir/d01_formatted_deaths_`country'.dta", replace
	}

	capture log close
	exit, clear
	