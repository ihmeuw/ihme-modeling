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

	if (c(os)=="Unix") global root "FILEPATH"
	if (c(os)=="Windows") global root "JFILEPATH"
	global input_file "FILEPATH/d03_combined_population_and_deaths.dta"
	global save_file "FILEPATH/d04_raw_ddm.dta"
	
	
** **********************
** Load DDM functions 
** **********************

	qui do "$function_dir/ddm.ado"
	qui do "$function_dir/ggb.ado"
	qui do "$function_dir/seg.ado"

** **********************
** Apply DDM 
** **********************

** both sexes
	use "$input_file", clear

	keep if sex == "both"
	tempfile ddmboth
	save `ddmboth', replace

	qui ddm, data("`ddmboth'") sex(0) trim_specific(0)
	g sex = "both"
	save `ddmboth', replace
	
** males
	use "$input_file", clear

	keep if sex == "male"
	tempfile ddmmale
	save `ddmmale', replace

	qui ddm, data("`ddmmale'") sex(1) trim_specific(0)
	g sex = "male"
	save `ddmmale', replace

** females
	use "$input_file", clear

	keep if sex == "female"
	tempfile ddmfemale
	save `ddmfemale', replace

	qui ddm, data("`ddmfemale'") sex(2) trim_specific(0)
	g sex = "female"
	save `ddmfemale', replace	

** **********************
** Compile DDM results and save
** **********************

	use `ddmboth', clear
	append using `ddmmale' `ddmfemale' 
	replace ihme_loc_id = substr(ihme_loc_id,1,strpos(ihme_loc_id, "&&")-1) if strpos(ihme_loc_id,"&&") ~= 0

	save "$save_file", replace


