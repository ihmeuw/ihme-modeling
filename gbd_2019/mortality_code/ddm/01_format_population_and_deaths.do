

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


	** Bring in cited compiled deaths and population data
	global pop_file FILEPATH
	global deaths_file FILEPATH

** **********************
** Load functions 
** **********************

	qui do "functions/orderednaming.ado"
	qui do "functions/onehundredyears.ado"
	
	set seed 1234 

	import delim using FILEPATH, clear
	keep if location_id == `loc_id'
	levelsof ihme_loc_id if location_id == `loc_id', local(country)


	** **********************
	** Format population
	** **********************
		
	noi: orderednaming, data("$pop_file") iso3(`country') type("pop") version("`version_id'")
	tempfile pop
	save `pop', replace

	noi: onehundredyears, data(`pop') varname("pop") iso3(`country')
	save FILEPATH, replace

	** **********************
	** Format deaths
	** **********************

	noi: orderednaming, data("$deaths_file") iso3(`country') type("deaths") version("`version_id'")
	tempfile deaths
	save `deaths', replace

	noi: onehundredyears, data(`deaths') varname("deaths") iso3(`country')
	save FILEPATH, replace

	capture log close
	exit, clear
	
