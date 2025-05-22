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
	set maxvar 32767
** **********************
** Filepaths 
** **********************

	local version_id `1'
	global function_dir "`2'"
	global main_dir "FILEPATH"
	global input_file "FILEPATH"
	global save_file "FILEPATH"
	
** **********************
** Load DDM functions 
** **********************

	qui do "$function_dir/ddm.ado"
	qui do "$function_dir/ggb.ado"
	qui do "$function_dir/seg.ado"
	qui do "$function_dir/ggbseg.ado"

** **********************
** Apply DDM 
** **********************

** both sexes
	use "$input_file", clear
	drop if time > 20

	keep if sex == "both"
	tempfile ddmboth
	save `ddmboth', replace

	qui ddm, data("`ddmboth'") sex(0) trim_specific(0)
	g sex = "both"
	save `ddmboth', replace
	
** males
	use "$input_file", clear
	drop if time > 20

	keep if sex == "male"
	tempfile ddmmale
	save `ddmmale', replace

	qui ddm, data("`ddmmale'") sex(1) trim_specific(0)
	g sex = "male"
	save `ddmmale', replace

** females
	use "$input_file", clear
	drop if time > 20

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


