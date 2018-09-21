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
		
		global out_dir = "FILEPATH"
		local code_dir "FILEPATH"
		di "`code_dir'"
		global function_dir "`code_dir'/functions"
	} 
	if (c(os)=="Windows") { 
		global root "FILEPATH"
		local group = "5" 
		local user = "`c(username)'"
		
		global out_dir = "FILEPATH"
		local code_dir "FILEPATH"
		global function_dir "FILEPATH" 
	} 

	global pop_file "FILEPATH/d00_compiled_population.dta"
	global deaths_file "FILEPATH/d00_compiled_deaths.dta"	

** **********************
** Load functions 
** **********************

	qui do "$function_dir/orderednaming.ado"
	qui do "$function_dir/onehundredyears.ado"
	qui do "FILEPATH/get_locations.ado"
	
	set seed 1234

	get_locations, gbd_type(ap_old) level(estimate)
	keep if location_name == "Old Andhra PrUSER"
	tempfile ap_old 
	save `ap_old', replace

	get_locations, gbd_year(2016) 
	drop if (regexm(ihme_loc_id, "KEN_") & level == 4)
	append using `ap_old'
	sort ihme_loc_id
	gen n = _n
	egen group = cut(n), group(150) // Make 75 equally sized groups for analytical purposes
	qui levelsof ihme_loc_id if group == `group', local(countries) 
	
	foreach country in `countries' {	
		** **********************
		** Format population
		** **********************
			
			noi: orderednaming, data("$pop_file") iso3("`country'") type("pop")
			tempfile pop
			save `pop', replace

			noi: onehundredyears, data(`pop') varname("pop") iso3("`country'")
			save "$out_dir/d01_formatted_population_`country'.dta", replace

		** **********************
		** Format deaths
		** **********************

			noi: orderednaming, data("$deaths_file") iso3("`country'") type("deaths")
			tempfile deaths
			save `deaths', replace

			noi: onehundredyears, data(`deaths') varname("deaths") iso3("`country'")
			save "$out_dir/d01_formatted_deaths_`country'.dta", replace
	}

	capture log close
	exit, clear
	