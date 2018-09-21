clear all
set more off

// Set directories
	if c(os) == "Windows" {
		global j "FILEPATH"
		set mem 1g
	}
	if c(os) == "Unix" {
		global j "FILEPATH"
		set mem 8g
	} 
	
// directories
local raw_data			"filepath"
local final_data		"filepath"
local mort_data			"filepath" 
local in_dir			"filepath"		
local death_dir_country "filepath" 	
local date 				"13June2016"	

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
	use "`in_dir'/compiled_altpop_death.dta", clear
	
	// keep if year==1990 | year==1995 | year==2000 | year==2005 | year==2010 | year==2016
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

		get_demographics, gbd_team(mort) clear
     	get_population, year_id(2016) location_id(`locations') sex_id(1 2) age_group_id(2 3 4 5) clear
// 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 27
     	merge m:1 location_id using `location_map', keep(3) nogen

     	sort location_ascii_name year_id age_group_id sex_id
     	keep ihme_loc_id year_id age_group_id sex_id pop_scaled 
     	order ihme_loc_id year_id sex_id age_group_id
     	rename pop_scaled gbd_pop

     	gen age = .
     	**replace age = 0.01 		if age_group_id == 3
     	replace age = 0.1 		if age_group_id == 4
     	replace age = 1			if age_group_id == 5

     	// keep if year==1990 | year==1995 | year==2000 | year==2005 | year==2010 | year==2016

     	drop if age == .

     	tempfile gbd_pops
     	save `gbd_pops', replace	

// Combine BF age pop and GBD age pop to get fractions of population		
	**recast double age, force
	get_location_metadata, location_set_id(22) clear
	keep if level >= 3
	keep location_id ihme_loc_id parent_id location_name sort_order path_to_top_parent
	rename ihme_loc_id iso3
	duplicates drop iso3, force
	tempfile location_map
	save `location_map', replace

	**this actually represents population by bf_ages
	use "`bf_age_pops'", clear
	
	merge m:1 iso3 year age sex using "`gbd_pops'"
		keep if _m==3 | _m==1 // m=1 are not ihme countries
		drop _m

	gen pct_mean = draw_mean / gbd_pop
	
	replace pct_mean=1 if pct_mean>1
	
	keep year sex bf_age age iso3 pct_mean
	
	replace bf_age="1_5" if bf_age=="1-5 months"
	replace bf_age="6_11" if bf_age=="6-11 months"
	replace bf_age="12_23" if bf_age=="1"

	// rescale so 1-5 + 6-11 = 1
	reshape wide pct_mean, i(year sex age iso3) j(bf_age, string)
	gen tot =  pct_mean1_5 + pct_mean6_11
	replace pct_mean1_5 = pct_mean1_5 / tot
	replace pct_mean6_11 = pct_mean6_11 / tot
	drop tot
	
	tostring age, force replace format(%12.3f)
	destring age, force replace 
	keep if year==1990 | year==1995 | year==2000 | year==2005 | year==2010 | year==2016

	rename year year_id	
	** don't currently care about age = 1
	**drop if age == 1
	
	save "`bf_data'/pct_bf_ages_2016.dta", replace
}

// Use exposure draws formatted in the previous step here
// get list of non-developing countries 
	**use "`country_codes'", clear/
	get_location_metadata, location_set_id(9) clear 
	keep ihme_loc_id developed
	gen gbd_developing = 1 if developed == "0"
	replace gbd_developing = 0 if developed == "1"

	tempfile developing
	save `developing', replace
	
// Non-exclusive breastfeeding categories
if 1==1 {

	// 0-5 months (all of age group .01, part of age group .1)
	local raw_data ""
	use "`raw_data'/all_cats_0to5.dta", clear
	// keep if year==1990 | year==1995 | year==2000 | year==2005 | year==2010 | year==2016
	
	// apply to age groups  .01, .1
	expand 2, gen(id)
	gen age=.01 if id==0
	replace age=.1 	if id==1
	drop id

	**gen age = .1

	tostring age, force replace format(%12.3f)
	destring age, force replace 
	
	// apply to both sexes
	drop sex 
	expand 2, gen(id)
	gen sex = 1 if id==0
	replace sex = 2 if id==1
	drop id

	** change "china w/o Hong Kong and Macao" back to "China" just to mesh with other vars
	replace location_id = 6 if location_id == 44533

	// apply national splits to their subnationals
	merge m:1 location_id using `location_map', keep(3)
		keep if _merge == 3
		drop _merge
		** create iso3 using ihme_loc_id
		** gen iso3 = substr(ihme_loc_id, 1, 3)
	
	// merge on pre-calculated percents (what % of gbd age group is represented by bf age)
	merge m:1 iso3 year sex age using "`bf_data'/pct_bf_ages_2016.dta", keep(1 3) 
	drop if age==1
	assert _m==1 | _m==3 // m=1 are age group .01 (because we use entire thing - don't need %)
	drop _m
	keep if year==1990 | year==1995 | year==2000 | year==2005 | year==2010 | year==2016
	
	// replace exposures with exposure * percent to get exposure over entire gbd age group for age group .1
	forvalues k=0/999 {
		quietly replace exp_cat1_`k' = exp_cat1_`k' * pct_mean1_5 if age==.1 // no bf
		quietly replace exp_cat2_`k' = exp_cat2_`k' * pct_mean1_5 if age==.1 // partial bf
		quietly replace exp_cat3_`k' = exp_cat3_`k' * pct_mean1_5 if age==.1 // predominant bf
		quietly replace exp_cat4_`k' = (1 - (exp_cat1_`k' + exp_cat2_`k' + exp_cat3_`k')) if age==.1 // exclusive bf

		** fix anomaly, revisit after redone to assure it has been resolved in the modeling steps
		**replace exp_cat4_`k' = 0 if location_id == 43877
		
		di in red "checking cat 1"
		assert exp_cat1_`k'>=0
		di in red "checking cat 2"
		assert exp_cat2_`k'>=0
		di in red "checking cat 3"
		assert exp_cat3_`k'>=0
		**di in red "checking cat 4"
		**assert exp_cat4_`k'>=0
	}
	drop pct_* 
	tostring age, force replace format(%12.3f)
	destring age, force replace 

	order iso3 year age sex, first
	rename (iso3 sex) (ihme_loc_id sex_id)
	keep ihme_loc_id location_name location_id year_id age_group_id sex_id age measure_id exp_cat*
	
	save "`final_data'/bf_exclusive_exposure_2016.dta", replace
	
}

	
// Disontinued Breastfeeding
if 1==1 {
	
	// 6-11 months (fraction of gbd age group .1)
	use "`raw_data'/all_cats_6to11.dta", clear
	keep if year==1990 | year==1995 | year==2000 | year==2005 | year==2010 | year==2016
	
	gen age = .1
	tostring age, force replace format(%12.3f)
	destring age, force replace 
	
	drop sex
	expand 2, gen(id)
	gen sex = 1 if id==0
	replace sex = 2 if id==1
	drop id
	
	tempfile first
	save `first', replace
	
	// 12-23 months (fraction of gbd age group 1)
	use "`raw_data'/all_cats_12to23.dta", clear
	keep if year==1990 | year==1995 | year==2000 | year==2005 | year==2010 | year==2016
	
	gen age = 1
	expand 2, gen(id)
	drop sex
	gen sex = 1 if id==0
	replace sex = 2 if id==1
	drop id
	
	append using `first'

	replace location_id = 6 if location_id == 44533

	// apply national splits to their subnationals
	merge m:1 location_id using `location_map'
		keep if _merge == 3
		drop _merge
		** create iso3 using ihme_loc_id
		** gen iso3 = substr(ihme_loc_id, 1, 3)
		//rename ihme_loc_id iso3

	preserve
		use "`bf_data'/pct_bf_ages_2016.dta", clear
		replace year_id = 2016 if year_id == 2015
		tempfile bf_pct
		save `bf_pct', replace
	restore

	merge m:1 iso3 year sex age using `bf_pct'
	drop if _m==1 // not IHME countries
	assert _m==3 
	drop _m
	
	// replace exposures with exposure * percent to get exposure over entire gbd age group
	forvalues k=0/999 {
		quietly replace exp_cat1_`k' = exp_cat1_`k' * pct_mean6_11 if age==.1
		quietly replace exp_cat1_`k' = exp_cat1_`k' * pct_mean12_23 if age==1
		quietly replace exp_cat2_`k' = 1 - exp_cat1_`k'
	}
	
	drop pct_*

	order iso3 year age sex, first
	tostring age, force replace format(%12.3f)
	destring age, force replace 
	
	drop if year==.
	
	save "`final_data'/bf_continued_exposure_`date'.dta", replace
}	


**********************************
*****END OF CODE******************
