** Project: RF: Suboptimal Breastfeeding
** Purpose: Redistribute breastfeeding exposures into GBD age groups

clear all
set more off

// directories
local raw_data			"FILEPATH"
local final_data		"FILEPATH"
local in_dir			"FILEPATH"		

adopath + "FILEPATH"

** create a location_map tempfile
	get_location_metadata, location_set_id(22) clear
	keep if level >= 3
	keep location_id ihme_loc_id location_ascii_name
	tempfile location_map
	save `location_map', replace

*****************************************************************************
********************BREASTFEEDING AGE GROUP POPULATIONS**********************
*****************************************************************************

** //Use pop numbers to get BF age pop as a fraction of total GBD age pop	
	use "FILEPATH", clear
	
	forvalues x=1/1000 {
		rename pys`x' draw_`x'
	}
	
//Reformat age variable for easier analysis later
	rename age bf_age
	gen age = .
	replace age = .1 if bf_age == "pna"
	replace age = .1 if bf_age == "pnb"
	replace age = 1 if bf_age == "1"
	drop if age == .
	replace bf_age = "1-5 months" if bf_age == "pna"
	replace bf_age = "6-11 months" if bf_age == "pnb"

	// fix sex to match gbd pop
	gen sex_id = .
	replace sex_id=1 if sex=="male"
	replace sex_id=2 if sex=="female"
	drop sex
	sort ihme_loc_id year sex_id age
	rename year year_id

	fastrowmean draw_*, mean_var_name(draw_mean)
	keep ihme_loc_id year sex_id age bf_age draw_mean

	//save for merging with gbd populations later
	tempfile bf_age_pops
	save `bf_age_pops', replace

***********************************************************
****************GBD AGE GROUP POPULATIONS******************
***********************************************************

// Get total deaths for GBD ages as the denominator
	** Get Population Estimates
		use `location_map', clear
		levelsof location_id, local(locations)
		local years 1980 1981 1982 1983 1984 1985 1986 1987 1988 1989 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017

		get_demographics, gbd_team(mort) clear
     	get_population, year_id(`years') location_id(`locations') sex_id(1 2) age_group_id(2 3 4 5) clear
		
     	merge m:1 location_id using `location_map', keep(3) nogen

     	sort location_ascii_name year_id age_group_id sex_id
     	keep ihme_loc_id year_id age_group_id sex_id population
     	order ihme_loc_id year_id sex_id age_group_id
     	rename population gbd_pop

     	gen age = .
     	**replace age = 0.01 		if age_group_id == 3
     	replace age = 0.1 		if age_group_id == 4
     	replace age = 1			 if age_group_id == 5

     	drop if age == .

     	tempfile gbd_pops
     	save `gbd_pops', replace	

// Combine BF age pop and GBD age pop to get fractions of population		
	get_location_metadata, location_set_id(22) clear
	keep if level >= 3
	keep location_id ihme_loc_id parent_id location_name sort_order path_to_top_parent
	duplicates drop ihme_loc_id, force
	tempfile location_map
	save `location_map', replace

	**this actually represents population by bf_ages
	use "FILEPATH", clear
	
	merge m:1 ihme_loc_id year_id age sex_id using "FILEPATH"
		keep if _m==3 | _m==1 
		drop _m

	gen pct_mean = draw_mean / gbd_pop
	
	keep year_id sex_id bf_age age ihme_loc_id pct_mean
	
	replace bf_age="1_5" if bf_age=="1-5 months"
	replace bf_age="6_11" if bf_age=="6-11 months"
	replace bf_age="12_23" if bf_age=="1"

	// rescale so 1-5 + 6-11 = 1
	reshape wide pct_mean, i(year_id sex_id age ihme_loc_id) j(bf_age, string)
	gen tot =  pct_mean1_5 + pct_mean6_11
	replace pct_mean1_5 = pct_mean1_5 / tot
	replace pct_mean6_11 = pct_mean6_11 / tot
	drop tot
	
	tostring age, force replace format(%12.3f)
	destring age, force replace 

	rename year year_id	
	
	save "FILEPATH", replace
}
	
// Non-exclusive breastfeeding categories
if 1==1 {

	// 0-5 months (all of age group .01, part of age group .1)
	use "FILEPATH", clear
	
	// apply to age groups  .01, .1
	expand 2, gen(id)
	gen age=.01 if id==0
	replace age=.1 	if id==1
	drop id

	tostring age, force replace format(%12.3f)
	destring age, force replace 
	
	// apply to both sexes
	drop sex_id
	expand 2, gen(id)
	gen sex_id = 1 if id==0
	replace sex_id = 2 if id==1
	drop id

	// apply national splits to their subnationals
	merge m:1 location_id using `location_map', keep(3)
		keep if _merge == 3
		drop _merge
	
	// merge on pre-calculated percents (what % of gbd age group is represented by bf age)
	merge m:1 ihme_loc_id year_id sex_id age using "FILEPATH", keep(1 3) 
	drop if age==1
	assert _m==1 | _m==3 
	drop _m
	
	// replace exposures with exposure * percent to get exposure over entire gbd age group for age group .1		
	forvalues k=0/999 {
		*no bf
		quietly replace exp_cat1_`k' = exp_cat1_`k' * pct_mean1_5 if age==.1 
		*partial bf
		quietly replace exp_cat2_`k' = exp_cat2_`k' * pct_mean1_5 if age==.1
		*predominant bf
		quietly replace exp_cat3_`k' = exp_cat3_`k' * pct_mean1_5 if age==.1 
		*exclusive bf
		quietly replace exp_cat4_`k' = (1 - (exp_cat1_`k' + exp_cat2_`k' + exp_cat3_`k')) if age==.1
		
		di in red "checking cat 1"
		assert exp_cat1_`k'>=0
		di in red "checking cat 2"
		assert exp_cat2_`k'>=0
		di in red "checking cat 3"
		assert exp_cat3_`k'>=0
	}
	drop pct_* 
	tostring age, force replace format(%12.3f)
	destring age, force replace 

	order ihme_loc_id year_id age sex_id, first
	rename (iso3 sex) (ihme_loc_id sex_id)
	keep ihme_loc_id location_id year_id age_group_id sex_id age measure_id exp_cat*
	
	save "FILEPATH", replace
	
}

	
// Disontinued Breastfeeding
if 1==1 {
	
	// 6-11 months (fraction of gbd age group .1)
	use "FILEPATH", clear
	
	gen age = .1
	tostring age, force replace format(%12.3f)
	destring age, force replace 
	
	drop sex_id
	expand 2, gen(id)
	gen sex_id = 1 if id==0
	replace sex_id = 2 if id==1
	drop id
	
	tempfile first
	save `first', replace
	
	// 12-23 months (fraction of gbd age group 1)
	use "FILEPATH", clear
	
	gen age = 1
	expand 2, gen(id)
	drop sex_id
	gen sex_id = 1 if id==0
	replace sex_id = 2 if id==1
	drop id
	
	append using `first'
	tostring age, force replace format(%12.3f)
	destring age, force replace 
	
	// apply national splits to their subnationals
	merge m:1 location_id using `location_map'
		keep if _merge == 3
		drop _merge
	
	// merge on pre-calculated percents (what % of gbd age group is represented by bf age)
	preserve
		use "FILEPATH", clear
		*replace year_id = 2016 if year_id == 2015
		tostring age, force replace format(%12.3f)
		destring age, force replace 
		tempfile bf_pct
		save `bf_pct', replace
	restore

	merge m:1 ihme_loc_id year_id sex_id age using `bf_pct'
	drop if _m==1 
	assert _m==3 
	drop _m
	
	// replace exposures with exposure * percent to get exposure over entire gbd age group
	forvalues k=0/999 {
		quietly replace exp_cat1_`k' = exp_cat1_`k' * pct_mean6_11 if age==.1
		quietly replace exp_cat1_`k' = exp_cat1_`k' * pct_mean12_23 if age==1
		quietly replace exp_cat2_`k' = 1 - exp_cat1_`k'
	}
	
	drop pct_*

	order ihme_loc_id year_id age sex_id, first
	tostring age, force replace format(%12.3f)
	destring age, force replace 
	
	drop if year==.
	
	save "FILEPATH", replace
}	


**********************************
*****END OF CODE******************
**********************************
