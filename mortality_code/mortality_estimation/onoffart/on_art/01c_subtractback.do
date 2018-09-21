// Subtract study specifc background mortality

// settings
	clear all
	set more off

	if (c(os)=="Unix") {
		global root FILEPATH
	}

	if (c(os)=="Windows") {
		global root FILEPATH
	}
	
	adopath + FILEPATH
	
// locals
	local HIV_free_mort FILEPATH  
	local GBD_pop FILEPATH
	local data_dir FILEPATH

// Get locations
	get_locations
	rename ihme_loc_id iso3
	tempfile country_codes
	save `country_codes' 
	
	
*bring in splitr KM data 
use  "`data_dir'/bradmod/split_km", clear  

//have to change some isos to proper format 
replace iso3 = "ARG, AUT, BLR, BEL, BIH, HRV, CZE, DNK, EST, FIN, FRA, DEU, GEO, GRC, HUN, ISL, IRL, ISR, ITA, NLD, RUS, GBR, UKR, CHE, SWE, ESP" if iso == "High-Income"                  
replace iso3 = "MAR, BWA, MWI, ZAF, KEN, CIV, NGA, SEN, BRA, IND, THA" if iso3 == "SSA"  

//WE RESHAPE, MERGE AND THEN COLLAPSE. WE WANT TO ENSURE THAT WE DO NOT LOSE OBSERVATIONS
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

*merge on year-country specific treatment eligibility. We want to assign more accurate cd4 ranges here to cohorts that start at 0 cd4 (some exceptions). We also want to outlier cohorts that have a cd4 start above 0 and end above the threshold that are not from high-income (some exceptions). 
preserve 
	insheet using "`data_dir'/cd4_caps.csv", clear  
	keep year cd4_threshold iso3 
	tempfile cd4e 
	save `cd4e', replace 
restore 


merge m:1 year iso3 using `cd4e', keep(1 3) nogen 

gen outlier = 0 
replace outlier = 1 if cd4_end>cd4_threshold & super != "high" 
replace cd4_end = cd4_threshold if outlier == 1 & cd4_start == 0 
replace outlier = 0 if outlier == 1 & cd4_start == 0

tempfile km
save `km', replace 

*now hiv free mort + pops  
 
use "`HIV_free_mort'", clear 
	
	// Keep vars and age groups of interest...  
		drop if age==0 | age==1 | age==105 | age==110 |  age==5 | age==10 | age == 100
		drop if regexm(ihme_loc_id,"_") & !inlist(ihme_loc_id,"CHN_44533","CHN_494", "CHN_354","CHN_361") 
		rename mean_mx mx_nohiv_mean
		rename ihme_loc_id iso3
		keep iso3 year age sex mx_nohiv_mean
		tempfile tmp_mort_prep
		save `tmp_mort_prep', replace 
	
	// Prep population
		use ihme_loc_id year_id sex_id age_group_id age_group_name pop using "`GBD_pop'" if sex_id != 3 & age_group_id <=20 & age_group_id >= 8 | inlist(age_group_id,30,31,32,33,48,235) , clear // Keep 15-100 populations and sex-specific 
		drop if missing(ihme)
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
		drop if age > 75
		gen HIV_dth_num=mx_nohiv*pop 
		
		gen subtract_age="15_25" if age>=15 & age<25
		replace subtract_age="25_35" if age>=25 & age<35
		replace subtract_age="35_45" if age>=35 & age<45
		replace subtract_age="45_55" if age>=45 & age<55
		replace subtract_age="55_100" if age>=55 
		
		collapse (sum) HIV_dth_num pop, by(iso3 year sex subtract_age) 
		
		//need to create 45-100 for some cohorts 
		expand 2 if inlist(subtract_age, "45_55", "55_100"), gen(IeDEA_45_100) 
		replace subtract_age = "45_100" if IeDEA_45_100 == 1 
		collapse (sum) HIV_dth_num pop, by(iso3 year sex subtract_age) 
		
		gen death_rate_nohiv=HIV_dth_num/pop   
		
		tempfile nohivrts 
		save `nohivrts', replace 
		
* now final merge  

use `km', clear 

gen subtract_age = age 
replace subtract_age = "45_100" if nid ==  & inlist(age,"45_55", "55_100")

merge m:1 iso3 year sex subtract_age using `nohivrts', keep(1 3)   
count if _m==1 
if `r(N)' != 0{ 
do not_all_isos_merged_on_bg_mort
}
drop _m

collapse (mean) death_rate_nohiv (sum) outlier (mean) cd4_end, by(pubmed_id nid subcohort_id super year time_lower time_upper original site cohort cd4_start sample_size sex age time_point  rate) 

//check to make sure that collapsing worked 
describe 
local ss2 = `r(N)' 
if "`ss1'" != "`ss2'"{ 
do some_obs_were_droped_in_collapse
}


*outlier cohorts that exceed iso-year specific cd4 thresholds in non high income settings with some exceptions
drop if outlier>0 

rename original iso3

rename rate all_rate
gen rate = all_rate - death_rate_nohiv 
replace rate = 0 if rate < 0  

tempfile master
save `master', replace

save "`data_dir'/bradmod/km_rates", replace
