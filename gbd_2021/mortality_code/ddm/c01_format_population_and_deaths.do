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
		global main_dir = "FILEPATH"
		global out_dir = "FILEPATH"
		global function_dir "`code_dir'/functions"
	} 

	** Bring in cited compiled deaths and population data
	global pop_file "FILEPATH"
	global deaths_file "FILEPATH"

** **********************
** Load functions 
** **********************

	qui do "$function_dir/orderednaming.ado"
	qui do "$function_dir/onehundredyears.ado"
	
	set seed 1234 

	import delim using "FILEPATH", clear
	keep if location_id == `loc_id'
	levelsof ihme_loc_id if location_id == `loc_id', local(country)


	** **********************
	** Format population
	** **********************
		
	noi: orderednaming, data("$pop_file") iso3(`country') type("pop") version("`version_id'")
	tempfile pop
	save `pop', replace

	noi: onehundredyears, data(`pop') varname("pop") iso3(`country')
	save "FILEPATH", replace

	** **********************
	** Format deaths
	** **********************

	noi: orderednaming, data("$deaths_file") iso3(`country') type("deaths") version("`version_id'")
	tempfile deaths
	save `deaths', replace

	noi: onehundredyears, data(`deaths') varname("deaths") iso3(`country')
	save "FILEPATH", replace

	capture log close
	exit, clear
	