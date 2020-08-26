// Author: NAME
// Created: 1/25/14
// Prep HIV-free life table estimates

// Updated by: NAME
// Updated date: Feb 25, 2014

// settings
	clear all
	set more off

	if (c(os)=="Unix") {
		global root "ADDRESS"
	}

	if (c(os)=="Windows") {
		global root "ADDRESS"
	}
	
	adopath + "FILEPATH"
	
// locals
	local HIV_free_mort "FILEPATH" 
	local extract_file "FILEPATH"
	local store_data "FILEPATH"
	local GBD_pop "FILEPATH"

// Get locations
	get_locations
	rename ihme_loc_id iso3
	tempfile country_codes
	save `country_codes'
	
*********************
// Create a list of the countries we got mortality data from
********************
	
	import excel using "`extract_file'", firstrow clear
	keep if baseline==1 & include==1
	
	gen super="ssa" if inlist(gbd_region, "Southern Sub-Saharan Africa", "Western Sub-Saharan Africa", "Eastern Sub-Saharan Africa", "Sub-Saharan Africa and Other", "Sub-Saharan Africa") ///
		| inlist(pubmed_id, 22972859, 16905784) | (pubmed_id==16530575 & site=="Low Income")
	replace super="other" if inlist(gbd_region, "Tropical Latin America", "North Africa and Middle East", "East Asia", "Australasia", "Southeast Asia", "Latin America and Caribbean", "Latin America/Caribbean" , "South Asia")
	replace super = "other" if inlist(gbd_region,"Latin America and the Carribean","Eastern Europe") 
	replace super = "other" if regexm(gbd_region, "Latin")
	replace super="high" if inlist(gbd_region, "High-Income", "Western Europe", "High-income North America") | (pubmed_id == 16530575 & site=="High Income")

	replace super="high" if iso=="ARG" | iso=="CHL" 
	
	
	gen best=1 if ///
	site=="Buhera Hospitals and Health Centres" | ///
	site=="rural clinic, Mbarara" | ///
	site=="HIV/AIDS care clinic in Johannesburg" | ///
	site=="Buhera Health Centres" | ///
	site=="41 healthcare facilities in Kigali City and the western province of Rwanda" | ///
	cohort=="OPERA" | ///
	cohort=="HIMS" | ///
	cohort=="CTAC" | ///
	cohort=="Gaborone Independent" 
	
	expand=2 if best==1, gen(ssabest)
	drop best
	replace super="ssabest" if ssabest==1

	replace year_start=year_end if year_start==.
	
	keep iso3 pubmed_id nid year_start year_end sample_size subcohort_id super prop_male
	gen year=floor((year_start+year_end)/2)
	split iso3, parse(, " ")
	rename iso3 original
	
	
	reshape long iso3, i(nid pubmed_id subcohort_id original year super prop_male) j(num)
	drop if iso==""
	drop num
	
	bysort pubmed_id nid subcohort_id original year: gen num_sites=_N
	replace sample_size=sample_size/num_sites

	gen sample_sizefemale=sample_size*(1-prop_male)
	gen sample_sizemale=sample_size*(prop_male)
	drop sample_size
	reshape long sample_size, i(pubmed_id nid subcohort_id year iso3 super) j(sex) string

	collapse (sum) sample_size, by(iso3 year super sex)
	replace sex="1" if sex=="male"
	replace sex="2" if sex=="female"
	destring sex, replace

	tempfile sample_sizes
	save `sample_sizes', replace
	
	
** *******************************
// Prep life tables
** *******************************


	// Prep HIV free mort rates
	use "`HIV_free_mort'", clear 
	
	// Keep vars and age groups of interest...  
	*dropping 100 since there are no 100 plus pops anymore I will use the 95 mort rate
		drop if age==0 | age==1 | age==105 | age==110 |  age==5 | age==10 | age == 100
		drop if regexm(ihme_loc_id,"_") & !inlist(ihme_loc_id,"CHN_44533", "CHN_354","CHN_361") // Don't double-count subnational locations
		rename mean_mx mx_nohiv_mean
		rename ihme_loc_id iso3
		keep iso3 year age sex mx_nohiv_mean
		tempfile tmp_mort_prep
		save `tmp_mort_prep', replace 
	
	// Prep population
	// Note: All countries except random new ones (ASM, etc) and ALL subnationals should have granular over-80 pops
	// But double check it before  
	*have to add 95+ for gbd 2016 since no more 100 plus, also now checking for over 80 granular is done at 90-94
		use ihme_loc_id year_id sex_id age_group_id age_group_name pop using "`GBD_pop'" if sex_id != 3 & age_group_id <=20 & age_group_id >= 8 | inlist(age_group_id,30,31,32,33,48,235) , clear // Keep 15-100 populations and sex-specific 
		rename year_id year
		di in red "These countries are missing the more granular pops"
		levelsof ihme_loc_id if age_group_id == 32 & pop == . & year == 2010
		rename (ihme_loc_id sex_id) (iso3 sex)
		
		replace age_group_name = subinstr(age_group_name," plus","",.)
		split age_group_name , p(" to ")
		drop age_group_name2
		rename age_group_name1 age
		destring age, replace 
		order iso3 year age sex pop 
		keep iso3 year sex age pop
		
		tempfile tmp_pop
		save `tmp_pop', replace 
	
	//  merge pop and mortality
		use "`tmp_mort_prep'", clear
		merge 1:1 iso3 year sex age using "`tmp_pop'", keep(3) nogen 
		// merge on GBD super region
		merge m:1 iso3 using `country_codes', keepusing(super_region_name) keep(1 3)
		replace super_region_name = "Sub-Saharan Africa" if iso3=="SSD"
		duplicates drop 
		drop _m
		// replace to BradMod regions
		gen super="high" if super_region_name=="High-income"
		replace super="ssa" if super_region_name=="Sub-Saharan Africa"
		replace super="other" if regexm(super_region_name, "Latin America") | regexm(super_region_name,"East Asia") | regexm(super_region_name, "North Africa") | regexm(super_region_name,"South Asia") | regexm(super_region_name,"Central Europe")
				
		// DO WE NEED SUB-NATIONAL?
		drop if super==""
	
	// generate death numbers
		gen HIV_dth_num=mx_nohiv*pop
		
	// generate spectrum age groups
		gen spectrum_age="15-25" if age>=15 & age<25
		replace spectrum_age="25-35" if age>=25 & age<35
		replace spectrum_age="35-45" if age>=35 & age<45
		replace spectrum_age="45-55" if age>=45 & age<55
		replace spectrum_age="55-100" if age>=55

	// duplicate all the ssa observations and change the super to 'ssabest' so i can merge with the top performers
		expand=2 if super=="ssa", gen(best)
		replace super="ssabest" if best==1
		
	// generate regional numbers for studies without one country
	preserve
		collapse (sum) HIV_dth_num pop, by(super year spectrum_age sex)
		gen iso3="SSA" if super=="ssa"
		replace iso3="HIGH" if super=="high"
		drop if iso3==""
		tempfile regional_numbers
		save `regional_numbers', replace
	restore
	
	append using `regional_numbers'

	collapse (sum) HIV_dth_num pop, by(iso3 year sex spectrum_age super)
	rename spectrum_age age
	
	merge m:1 iso3 year sex super using `sample_sizes'

	keep if _m==3
	drop _m
	
	*** NOW COLLAPSE, WEIGHTING BY THE SAMPLE SIZE FOR EACH SEX, TO GET THE HIV FREE SEX-AGE-SPECIFIC DEATH RATE
	
	gen death_rate=HIV_dth_num/pop
	gen num_deaths_studies=death_rate*sample_size

	collapse (sum) num_deaths_studies sample_size, by(super age sex)
	
	gen HIV_free_dth_rt=(num_deaths_studies/sample_size)
	sort age super sex


	// generate death rates
		keep if super=="high" | super=="ssa" | super=="other" | super=="ssabest"
		order super age sex 
		drop num_deaths_studies sample_size
		outsheet using "`store_data'/HIV_free_spectrum_dth_rts.csv", delim(",") replace 	



