********************************************************
** Description:
** Formats population and deaths data.
**
**
**
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
		local loc_id `1'
		local version_id `2'
		local gbd_year `3'
		local code_dir `4'
		global main_dir "FILEPATH"
		global out_dir = "FILEPATH"
		global function_dir "`code_dir'/functions"

	} 
	if (c(os)=="Windows") { 
		global root "J:"
		local group = "5" 
		local user = "`c(username)'"
		local code_dir "FILEPATH"
        global function_dir "`code_dir'/functions"

        global out_dir = "FILEPATH"
	} 

** **********************
** Load functions 
** **********************	

	qui do "$function_dir/reshape_pop.ado"
	qui do "$function_dir/reshape_deaths.ado"
	qui do "$function_dir/select_registry_years.ado"

	set seed 1234
	
	import delim using "FILEPATH", clear
	keep if location_id == `loc_id'
	levelsof ihme_loc_id if location_id == `loc_id', local(country)

	global pop_file "FILEPATH"
	global deaths_file "FILEPATH"		
	global save_pop_file "FILEPATH"
	global save_deaths_file "FILEPATH"
	
	** **********************
	** Reshape population
		noi: reshape_pop, data("$pop_file") iso3(`country') saveas("$save_pop_file")
		
	** **********************
	** Reshape deaths
	** **********************
		noi: reshape_deaths, popdata("$save_pop_file") data("$deaths_file") iso3(`country') saveas("$save_deaths_file")
	
	capture log close
	exit, clear
