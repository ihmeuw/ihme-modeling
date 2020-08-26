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
	qui do "FILEPATH" 
	*outlier cd4 threshold exceeding cohorts
	local outlier = 0
	
// locals
	local HIV_free_mort "FILEPATH"
	local extract_file "FILEPATH"
	local store_data "FILEPATH"
	local GBD_pop "FILEPATH"
	local data_dir "FILEPATH"

// Get locations
	get_locations
	rename ihme_loc_id iso3
	tempfile country_codes
	save `country_codes' 
	
//Get age map
	get_age_map, type("all") 
	tempfile age_map
	save `age_map', replace
	
	
	
*bring in splitr KM data 
use  "`data_dir'/FILEPATH/split_km", clear  

//have to change some isos to proper format 
replace iso3 = "ARG, AUT, BLR, BEL, BIH, HRV, CZE, DNK, EST, FIN, FRA, DEU, GEO, GRC, HUN, ISL, IRL, ISR, ITA, NLD, RUS, GBR, UKR, CHE, SWE, ESP" if iso == "High-Income"                  
replace iso3 = "MAR, BWA, MWI, ZAF, KEN, CIV, NGA, SEN, BRA, IND, THA" if iso3 == "SSA"  

//going to save this in macro to make sure that the collapsing code below works
describe 
local ss1 = `r(N)' 



gen year=floor((time_lower+time_upper)/2)
split iso3, parse(, " ")
rename iso3 original
	
	
gen id = _n	
reshape long iso3, i(id) j(num)
drop if iso==""
drop num  
drop rownum  

replace sex="1" if sex=="male"
replace sex="2" if sex=="female"
destring sex, replace  

*merge on year-iso specific treatment eligibility 
preserve 
	insheet using "`data_dir'/cd4_caps.csv", clear  
	keep year cd4_threshold iso3 
	replace cd4_threshold = 1000 if cd4_threshold == 999
	expand 2 if iso3 == "CHN", gen(mainland) 
	replace iso3 = "CHN_44533" if mainland == 1 
	drop mainland
	tempfile cd4e 
	save `cd4e', replace 
restore 


merge m:1 year iso3 using `cd4e', keep(1 3) nogen 

//set cd4 ranges (already done earlier but this country specific)
*dont want a rounded year for this 
gen myear = (time_lower + time_upper)/2
*if the threshold is missing and countrie is high income set to 500 
replace cd4_threshold = 500 if super == "high" & missing(cd4_threshold)
*outside high income
replace cd4_threshold = 200 if super != "high" & myear < 2010 & missing(cd4_threshold) 
replace cd4_threshold = 350 if super != "high" & myear >= 2010 & myear < 2013 & missing(cd4_threshold)
replace cd4_threshold = 500 if super != "high" & myear >= 2013  & missing(cd4_threshold) 

replace cd4_end = cd4_threshold if cd4_specificity == "unspecified" 
replace cd4_end = cd4_threshold if cd4_start < cd4_threshold & cd4_specificity == "lower specified"
replace cd4_end = 500 if cd4_start >= cd4_threshold & cd4_start < 500 & cd4_specificity == "lower specified"
replace cd4_end = 1000 if cd4_start >= cd4_threshold & cd4_start >= 500 & cd4_specificity == "lower specified" 

//outlier
gen outlier = 0 
replace outlier = 1 if cd4_specificity == "unspecified" 
replace outlier = 1 if cd4_specificity == "lower specified" & cd4_end != 1000
replace outlier = 1 if cd4_specificity == "specified" & cd4_end > cd4_threshold
replace outlier = 1 if cd4_specificity == "lower specified" & cd4_end == 1000 & cd4_end > cd4_threshold
*some exceptions
*close enough to universal treatment
replace outlier = 0 if regexm(cohort,"IeDEA North America")
*reasonably assume everyone is sick
replace outlier = 0 if super == "ssa" & cd4_start >= 500
replace outlier = 0 if super == "other" & cd4_start >= 500
*a total indicator so we can see how many countries in cohort are outliered
gen tot = 1 



merge m:1 iso3 using `country_codes', keep(1 3) keepusing(location_id) nogen

levelsof location_id, local(isoids)

tempfile km
save `km', replace 

*now hiv free mort + pops  
 
insheet using "`HIV_free_mort'", clear 
drop if ihme == "NA"
	
	// Keep vars and age groups of interest...  
	*dropping 100 since there are no 100 plus pops anymore I will use the 95 mort rate
		rename mean_mx mx_nohiv_mean
		rename ihme_loc_id iso3
		keep iso3 year age sex mx_nohiv_mean 
		//235 is 95+ and 33 is 95-99 but they do not match up so this is only way, we dont use it currently anyway
		replace age_group_id = 235 if age_group_id == 33
		tempfile tmp_mort_prep
		save `tmp_mort_prep', replace 
	
	// Prep population
		*use ihme_loc_id year_id sex_id age_group_id age_group_name pop using "`GBD_pop'" if sex_id != 3 & age_group_id <=20 & age_group_id >= 8 | inlist(age_group_id,30,31,32,33,48,235) , clear // Keep 15-100 populations and sex-specific 
		get_population, location_id("`isoids'") sex_id("1 2") year_id("1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017") age_group_id("8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235") status("recent") clear
		merge m:1 location_id using `country_codes', keep(1 3) keepusing(iso3) nogen

		rename (sex_id year_id) (sex year)
		
		order iso3 year age sex pop 
		keep iso3 year sex age pop
		
		tempfile tmp_pop
		save `tmp_pop', replace 
	
	//  merge pop and mortality
		use "`tmp_mort_prep'", clear
		*have to add check here to make sure no _m==2 when 2017 comes
		merge 1:1 iso3 year sex age_group_id using "`tmp_pop'", keep(3) nogen 
		merge m:1 age_group_id using `age_map', keep(1 3) keepusing(age_group_years_start) nogen
		rename age_group_years_start age
		drop if age > 75
		gen HIV_dth_num=mx_nohiv*pop 
		
		gen subtract_age="15_25" if age>=15 & age<25
		replace subtract_age="25_35" if age>=25 & age<35
		replace subtract_age="35_45" if age>=35 & age<45
		replace subtract_age="45_55" if age>=45 & age<55
		replace subtract_age="55_100" if age>=55 
		
		collapse (sum) HIV_dth_num pop, by(iso3 year sex subtract_age) 
		
		//need to create 45-100 for IeDEA data 
		expand 2 if inlist(subtract_age, "45_55", "55_100"), gen(IeDEA_45_100) 
		replace subtract_age = "45_100" if IeDEA_45_100 == 1 
		collapse (sum) HIV_dth_num pop, by(iso3 year sex subtract_age) 
		
		gen death_rate_nohiv=HIV_dth_num/pop   
		
		tempfile nohivrts 
		save `nohivrts', replace 
		
* now final merge  

use `km', clear 

gen subtract_age = age 

merge m:1 iso3 year sex subtract_age using `nohivrts', keep(1 3)   
count if _m==1 
if `r(N)' != 0{ 
do not_all_isos_merged_on_bg_mort
}
drop _m

collapse (mean) death_rate_nohiv (mean) cd4_end (sum) outlier (sum) tot, by(pubmed_id nid subcohort_id super year time_lower time_upper original site cohort cd4_start sample_size sex age time_point  rate cd4_specificity) 

//check to make sure that collapsing worked 
describe 
local ss2 = `r(N)' 
if "`ss1'" != "`ss2'"{ 
	do some_obs_were_droped_in_collapse
}


*outlier cohorts that exceed iso-year specific cd4 thresholds in non high income settings with some exceptions, going to stop doing this
gen outlier_pct = outlier/tot

if `outlier'{ 
	drop if outlier > 0 

}



rename original iso3


rename rate all_rate
gen rate = all_rate - death_rate_nohiv 
replace rate = 0 if rate < 0  

tempfile master
save `master', replace

*replaceing sample_sizes of india with study specific  
import excel "FILEPATH", firstrow sheet(Kaplan-Meier) clear  
keep if NID == 1993
drop if baseline == 1 
rename NID nid

keep nid sex age_start age_end cd4_start cd4_end sample_size 
duplicates drop nid sex age_start age_end cd4_start cd4_end sample_size, force
 
replace sex = "1" if sex == "male" 
replace sex = "2" if sex == "female" 
destring sex, replace 
replace age_end = age_end+1 if age_end != 100 
replace cd4_end = cd4_end+1 if cd4_end ==99 | cd4_end == 199  | cd4_end == 349 
replace cd4_end = 1000 if cd4_end == 1500
gen age = string(age_start)+"_"+string(age_end)  
rename sample_size sample_size_india

tempfile samples 
save `samples', replace 

use `master', clear
merge m:1 nid sex age cd4_start cd4_end using `samples', keepusing(sample_size_india) nogen 
replace sample_size = sample_size_india if nid == 1993 
drop sample_size_india 


save "`data_dir'/FILEPATH/km_rates", replace
