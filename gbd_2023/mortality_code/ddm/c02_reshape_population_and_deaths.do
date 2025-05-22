********************************************************
** Description:
** Formats population and deaths data.
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
		local with_shock `2'
		local version_id `3'
		local gbd_year `4'
		local code_dir `5'
		global main_dir "FILEPATH"
		global out_dir = "FILEPATH"
		global function_dir "FILEPATH"

	}
	if (c(os)=="Windows") {
		global root "FILEPATH"
		local group = "5"
		local user = "`c(username)'"
		local code_dir "FILEPATH"
        global function_dir "FILEPATH"

        global out_dir = "FILEPATH"
	}

** **********************
** Load functions
** **********************

	qui do "FILEPATH"
	qui do "FILEPATH"
	qui do "FILEPATH"

	set seed 1234

	import delim using "FILEPATH", clear
	keep if location_id == `loc_id'
	levelsof ihme_loc_id if location_id == `loc_id', local(country)

	global pop_file "FILEPATH"

	if ("`with_shock'" == "1") {
	global deaths_file "FILEPATH"
	global save_deaths_file "FILEPATH"
	global save_pop_file "FILEPATH"
	}
	else {
		global deaths_file "FILEPATH"
	  global save_deaths_file "FILEPATH"
	  global save_pop_file "FILEPATH"
	}

	** **********************
	** Reshape population
		noi: reshape_pop, data("$pop_file") iso3(`country') saveas("$save_pop_file")

	** **********************
	** Reshape deaths
	** **********************
		noi: reshape_deaths, popdata("$save_pop_file") data("$deaths_file") iso3(`country') saveas("$save_deaths_file")

	capture log close
	exit, clear
