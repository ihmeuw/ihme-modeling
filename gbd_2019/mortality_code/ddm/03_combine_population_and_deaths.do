
** **********************
** Set up Stata 
** **********************

	clear all
	capture cleartmp 
	set mem 500m
	set more off
	local loc_id `1' 

** **********************
** Load functions 
** **********************		
	qui do "functions/combine_reshaped.ado"

	set seed 1234

	import delim using LOCATIONS, clear
	keep if location_id == `loc_id'
	levelsof ihme_loc_id if location_id == `loc_id', local(country)

** **********************
** Combine reshaped population and deaths
** **********************

	global pop_file = FILEPATH
	global deaths_file = FILEPATH
	global save_file FILEPATH

	noi: combine_reshaped, popdata("$pop_file") deathsdata("$deaths_file") saveas("$save_file")
	
	exit, clear
