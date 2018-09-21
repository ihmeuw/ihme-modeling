********************************************************
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
	
	if (c(os)=="Unix") global root "FILEPATH"
	if (c(os)=="Windows") global root "FILEPATH"
    if "$end_year" == "" global end_year 2016
	
	global ddm_pop_file "FILEPATH/d01_formatted_population.dta"
	global save_file "FILEPATH/d09_denominators.dta"
	
	adopath + "FILEPATH"
	adopath + "FILEPATH"

** **********************
** Set up country codes file 
** **********************
	get_locations, gbd_type(ap_old) level(estimate)
	keep if location_name == "Old Andhra PrUSER"
	tempfile ap_old 
	save `ap_old', replace

	get_locations, gbd_year(2016)
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


** **********************
** Format IHME National Populations 
** **********************
	get_population, location_id("-1") location_set_version_id("159") status("recent") age_group_id("-1") year_id("-1") sex_id("-1") clear 
	drop process
	rename year_id year
	merge m:1 location_id using `codes', keep(3) nogen
	merge m:1 age_group_id using `agemap', keep(3) nogen
	drop if regexm(age_group_name, "Neonatal")
	drop if age_group_id == 22
	drop if inlist(age_group_name, "All Ages", "15-49 years", "5-14 years", "70+ years", "Under 5", "50-69 years")
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


** **********************
** Format source-specific populations 
** **********************

	use "$ddm_pop_file", clear
    
    drop if source_type == "CENSUS" & ihme_loc_id != "CHN_44533" & !regexm(ihme_loc_id,"CHN_") & !regexm(ihme_loc_id,"MEX_") & !regexm(ihme_loc_id,"GBR_")  // Keep all GBD2010 subnational census data 
    drop if source_type == "CENSUS" & ihme_loc_id == "GBR_4749" // Drop England, if it's there
    drop if ihme_loc_id == "USA_531" & pop_source == "USA_CENSUS"
	replace source_type = "SRS" if regexm(source_type, "SRS")==1
	replace source_type = "DSP" if regexm(source_type, "DSP")==1
	drop month day pop_footnote agegroup* 
	
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
	keep ihme_loc_id country sex year source_type pop_source c1* pop_nid
	
	append using `ihme'
	drop country*
	
** **********************
** Make country-specific changes 
** **********************

** Oman: For the year 2009, deaths were gathered until July 2009. Because of this, we must multiply the number of 
**            person years by 7/12, or .583. This will give us the person-years for that fractional year. For the year 2004, 
**            deaths were gathered from May onward. Because of this, we must multiply the number of
**            person years by 8/12, or .666. This will give us the person-years for that fractional year.
	foreach newpop of varlist c1* {
		replace `newpop'=`newpop'*.666 if ihme_loc_id=="OMN" & year==2004
	}
	
** **********************
** Format and save file 
** **********************
	
** get regions and country names
	merge m:1 ihme_loc_id using `codes'
	keep if _m == 3
	drop _m 
	
** fill out sex variables
	replace sex = "male" if mi(sex) & sex_id == 1
	replace sex = "female" if mi(sex) & sex_id == 2
	replace sex = "both" if mi(sex) & sex_id == 3

	replace sex_id = 1 if mi(sex_id) & sex == "male"
	replace sex_id = 2 if mi(sex_id) & sex == "female"
	replace sex_id = 3 if mi(sex_id) & sex == "both"


** get both sexes if it's missing 
	preserve
	drop if sex == "both"
	gen count = 1
	collapse (sum) c1* count, by(region_name location_name location_id ihme_loc_id year source_type pop_source pop_nid)
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
	keep location_id location_name region_name ihme_loc_id year sex source_type pop_source pop_nid c*
    order location_id location_name region_name ihme_loc_id year sex source_type pop_source pop_nid c*
	sort ihme_loc_id year sex source_type
	saveold "$save_file", replace

