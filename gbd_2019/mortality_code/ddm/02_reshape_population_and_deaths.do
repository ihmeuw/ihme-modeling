

** **********************
** Set up Stata 
** **********************

	clear all
	capture cleartmp 
	set mem 500m
	set more off


** **********************
** Load functions 
** **********************	

	qui do "functions/reshape_pop.ado"
	qui do "functions/reshape_deaths.ado"
	qui do "functions/select_registry_years.ado"

	local loc_id `1'

	set seed 1234
	
	import delim using FILEPATH, clear
	keep if location_id == `loc_id'
	levelsof ihme_loc_id if location_id == `loc_id', local(country)

	global pop_file FILEPATH
	global deaths_file FILEPATH		
	global save_pop_file FILEPATH
	global save_deaths_file FILEPATH


	
	** **********************
	** Reshape population
		noi: reshape_pop, data("$pop_file") iso3(`country') saveas("$save_pop_file")
		
	** **********************
	** Reshape deaths
	** **********************
		noi: reshape_deaths, popdata("$save_pop_file") data("$deaths_file") iso3(`country') saveas("$save_deaths_file")
	
	capture log close
	exit, clear
