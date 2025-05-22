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
		global main_dir = "FILEPATH"
		global out_dir = "FILEPATH"
		global function_dir "FILEPATH"
	}

	** Bring in cited compiled deaths and population data
	global pop_file "FILEPATH"

	if ("`with_shock'" == "1") {
	global deaths_file "FILEPATH"
	global output_file "FILEPATH"
	}
	else {
		global deaths_file "FILEPATH"
		global output_file "FILEPATH"
	}

** **********************
** Load functions
** **********************

	qui do "FILEPATH"
	qui do "FILEPATH"

	set seed 1234

	import delim using "FILEPATH", clear
	keep if location_id == `loc_id'
	levelsof ihme_loc_id if location_id == `loc_id', local(country)


	** **********************
	** Format population
	** **********************

  if ("`with_shock'" == "0") {
	  noi: orderednaming, data("$pop_file") iso3(`country') type("pop") version("`version_id'") shocks("`with_shock'")
	  tempfile pop
	  save `pop', replace

	  noi: onehundredyears, data(`pop') varname("pop") iso3(`country')
	  save "FILEPATH", replace
	}

	** **********************
	** Format deaths
	** **********************

	noi: orderednaming, data("$deaths_file") iso3(`country') type("deaths") version("`version_id'") shocks("`with_shock'")
	tempfile deaths
	save `deaths', replace

	noi: onehundredyears, data(`deaths') varname("deaths") iso3(`country')
	save "$output_file", replace

	capture log close
	exit, clear
