********************************************************
** Author: 
** Date created: August 10, 2009
** Description: Compile populations to be used as denominators 
** 
** NOTE: IHME OWNS THE COPYRIGHT
********************************************************

** **********************
** Set up Stata 
** **********************

	clear all
	capture cleartmp 
    cap restore, not
	set mem 500m
	set more off

** **********************
** Filepaths 
** **********************
	
	if (c(os)=="Unix") global root "/home/j"	
	if (c(os)=="Windows") global root "J:"
    if "$end_year" == "" global end_year 2016
	
 	local version_id `1'
 	local gbd_year `2'
 	global main_dir "FILEPATH"
	global ddm_pop_file "$main_dir/FILEPATH/d01_formatted_population.dta"
	global save_file "$main_dir/FILEPATH/d09_denominators.dta"
	global ihme_pop_file "$main_dir/FILEPATH/gbd_population_estimates.dta"
	adopath + "FILEPATH"
	adopath + "FILEPATH"

** **********************
** Set up country codes file 
** **********************
	get_locations, gbd_type(ap_old) level(estimate) gbd_year(`gbd_year')
	keep if inlist(ihme_loc_id, "IND_44849", "XSU", "XYG")
	tempfile ap_old 
	save `ap_old', replace

	get_locations, gbd_year(`gbd_year')
	append using `ap_old'
	keep location_name ihme_loc_id region_name location_id
	tempfile codes
	save `codes', replace

** **********************
** Get age map
** **********************
	get_age_map, type(all)
	keep age_group_name age_group_id
	tempfile agemap
	save `agemap', replace

	
	use "$ihme_pop_file", clear
	rename year_id year


	merge m:1 location_id using `codes', keep(3) nogen
	merge m:1 age_group_id using `agemap', keep(3) nogen
	drop if regexm(age_group_name, "Neonatal")
	drop if age_group_id == 22
	drop if inlist(age_group_name, "All Ages", "15-49 years", "5-14 years", "70+ years", "Under 5", "50-69 years", "80 plus", "<20 years", "10 to 24")
	drop age_group_id location_name region_name
	replace age_group_name = "0to0" if age_group_name == "<1 year"
	replace age_group_name = subinstr(age_group_name," ","",.)

	rename pop c1_
	gen pop_source = "Maya's pop"
	reshape wide c1_, i(ihme_loc_id year sex pop_source) j(age_group_name) string
	gen source_type = "IHME"
	gen c1_0to4 = c1_0to0 + c1_1to4

	tempfile ihme
	save `ihme', replace


	use "$ddm_pop_file", clear
    
    drop if source_type == "CENSUS" & ihme_loc_id != "CHN_44533" & !regexm(ihme_loc_id,"CHN_") & !regexm(ihme_loc_id,"MEX_") & !regexm(ihme_loc_id,"GBR_") 
    drop if source_type == "CENSUS" & ihme_loc_id == "GBR_4749"
    drop if ihme_loc_id == "USA_531" & pop_source == "USA_CENSUS"
	replace source_type = "SRS" if regexm(source_type, "SRS")==1
	replace source_type = "DSP" if regexm(source_type, "DSP")==1
	drop precise_year pop_footnote agegroup* 
	
	drop if source_type == "SRS" & inlist(ihme_loc_id, "PAK", "BGD") 
	
	gen c1_0to0 = pop0
	gen c1_1to4 = pop1+pop2+pop3+pop4
	gen c1_0to4 = c1_0to0 + c1_1to4
	forvalues j=5(5)80 { 
		local j_plus = `j'+4
		egen c1_`j'to`j_plus' = rowtotal(pop`j'-pop`j_plus')
	} 
	egen c1_80plus = rowtotal(pop80-pop100)
	drop pop0-pop100
	keep ihme_loc_id country sex year source_type pop_source c1* pop_nid underlying_pop_nid
	
	append using `ihme'
	drop country*

	foreach newpop of varlist c1* {
		replace `newpop'=`newpop'*.666 if ihme_loc_id=="OMN" & year==2004
	}
	
** **********************
** Format and save file 
** **********************
	
	merge m:1 ihme_loc_id using `codes'
	keep if _m == 3
	drop _m 
	
	replace sex = "male" if mi(sex) & sex_id == 1
	replace sex = "female" if mi(sex) & sex_id == 2
	replace sex = "both" if mi(sex) & sex_id == 3

	replace sex_id = 1 if mi(sex_id) & sex == "male"
	replace sex_id = 2 if mi(sex_id) & sex == "female"
	replace sex_id = 3 if mi(sex_id) & sex == "both"


	preserve
	drop if sex == "both"
	gen count = 1
	collapse (sum) c1* count, by(region_name location_name location_id ihme_loc_id year source_type pop_source pop_nid underlying_pop_nid)
	keep if count == 2
	drop count 
	gen sex = "both"
	tempfile both
	save `both'
	restore
	
	gen temp = 1
	append using `both' 
	duplicates tag ihme_loc_id location_id year sex source_type, gen(d)
	drop if d > 0 & temp == . 
	drop d temp
	
** save
	keep location_id location_name region_name ihme_loc_id year sex source_type pop_source pop_nid underlying_pop_nid c*
    order location_id location_name region_name ihme_loc_id year sex source_type pop_source pop_nid underlying_pop_nid c*
	sort ihme_loc_id year sex source_type
	saveold "$save_file", replace

	save "FILEPATH/d09_denominators_v`version_id'", replace

* DONE