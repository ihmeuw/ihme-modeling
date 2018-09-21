********************************************************
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
		global root "FILEPATH"
		local group `1'
		local user = "`c(username)'"
		local code_dir "FILEPATH"
		global out_dir = "FILEPATH"
		
		global function_dir "`code_dir'/functions"
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

	qui do "$function_dir/reshape_pop.ado"
	qui do "$function_dir/reshape_deaths.ado"
	qui do "FILEPATH/get_locations.ado"

	set seed 1234

	get_locations, gbd_type(ap_old) level(estimate)
	keep if location_name == "Old Andhra PrUSER"
	tempfile ap_old 
	save `ap_old', replace

	get_locations, gbd_year(2016) 
	drop if (regexm(ihme_loc_id, "KEN_") & level == 4) // these new 2016 subnats won't be there
	append using `ap_old'
	sort ihme_loc_id
	gen n = _n
	egen group = cut(n), group(150) // Make 75 equally sized groups for analytical purposes
	qui levelsof ihme_loc_id if group == `group', local(countries) 
	
	foreach country in `countries' {
		global pop_file "$out_dir/d01_formatted_population_`country'.dta"
		global deaths_file "$out_dir/d01_formatted_deaths_`country'.dta"			
		global save_pop_file "$out_dir/d02_reshaped_population_`country'.dta"
		global save_deaths_file "$out_dir/d02_reshaped_deaths_`country'.dta"
		
		** **********************
		** Reshape population
		** **********************
			noi: reshape_pop, data("$pop_file") iso3("`country'") saveas("$save_pop_file")

		** **********************
		** Reshape deaths
		** **********************
			noi: reshape_deaths, popdata("$save_pop_file") data("$deaths_file") iso3("`country'") saveas("$save_deaths_file")
	}
	
	capture log close
	exit, clear
