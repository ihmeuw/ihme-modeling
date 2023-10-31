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
		global root "FILEPATH"
		local loc_id `1' 
		local version_id `2'
		local gbd_year `3'
		local code_dir `4'
		global function_dir "`code_dir'/functions"
		global main_dir "FILEPATH"
		global out_dir = "FILEPATH"
	} 
	if (c(os)=="Windows") {     
        global root "FILEPATH"
		local group = "5" 
		local user = "`c(username)'"
		local code_dir "FILEPATH"
		global function_dir "`code_dir'/functions"
		global out_dir = "FILEPATH"
	} 

** **********************
** Load functions 
** **********************		
	qui do "$function_dir/combine_reshaped.ado"

	set seed 1234

	import delim using "FILEPATH", clear
	keep if location_id == `loc_id'
	levelsof ihme_loc_id if location_id == `loc_id', local(country)

** **********************
** Combine reshaped population and deaths
** **********************

	global pop_file = "FILEPATH"
	global deaths_file = "FILEPATH"
	global save_file "FILEPATH"

	noi: combine_reshaped, popdata("$pop_file") deathsdata("$deaths_file") saveas("$save_file")
	
	exit, clear
