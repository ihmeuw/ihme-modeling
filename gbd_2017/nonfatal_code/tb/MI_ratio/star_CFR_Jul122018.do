
 
clear all
set more off
local date 07122018


insheet using "FILEPATH\locations_four_plus_stars.csv", comma names clear
rename ihme_loc_id iso3
gen star_4_5=1
tempfile star
save `star', replace


insheet using "FILEPATH\High_SDI_locations.csv", comma names clear
merge m:1 location_id using "FILEPATH\location_22.dta", keepusing(ihme_loc_id) keep(3)nogen
split ihme_loc_id, p(_)
rename ihme_loc_id1 iso3
duplicates drop iso3, force
keep iso3
gen high_sdi=1
tempfile high_sdi
save `high_sdi', replace

use "FILEPATH\SUP_xbs_TB_CNs_squeezed.dta", clear 
keep iso3 year age sex_id CN_bact_xb 
rename CN_bact_xb inc_cases
drop if year<2006
tempfile CN_1
save `CN_1', replace

use "FILEPATH\CNs_bact_2015_squeezed.dta", clear
rename sex sex_id
rename CN_bact_2015 inc_cases
split age, p(-)
drop age age2
destring age1, gen(age)
keep iso3 year age sex_id inc_cases

append using `CN_1'

tempfile inc
save `inc', replace


// Taiwan

insheet using "FILEPATH\TWN_notifications.csv", comma names clear

// drop 2015 due to incomplete data

drop if year_start==2015

rename ihme_loc_id iso3

gen year=year_start

gen sex_id=.
replace sex_id=1 if sex=="Male"
replace sex_id=2 if sex=="Female"

keep iso3 year sex_id age_start cases

            preserve
				qui keep if age_start>=65
				collapse (sum) cases, by(iso3 year sex_id)
				qui gen age=65
				tempfile tmp_65
				save `tmp_65', replace
			restore
		// create under 5 age group
			preserve
			    qui keep if age_start<=4
				collapse (sum) cases, by(iso3 year sex_id)
				qui gen age=0
				tempfile tmp_0
				save `tmp_0', replace
			restore
		// create custome age groups 5-15, 15-25, ..., 55-65	
			drop if age_start<=4
			drop if age_start>=65
				forvalues i=5(10)55 {
				preserve
					local k=`i'+5
					keep if age_start>=`i' & age_start<=`k'
					collapse (sum) cases, by(iso3 year sex_id)
					gen age=`i'
					tempfile tmp_`i'
					save `tmp_`i'', replace 
				restore
			}
			//append the files
			use "`tmp_0'", clear
			forvalues i=5(10)55 {
				qui append using "`tmp_`i''"
			}
			
			append using "`tmp_65'"
			
			rename cases inc_cases
			
tempfile TWN
save `TWN', replace


// BRA
use "FILEPATH\BRA_TB_tabulated.dta", clear

rename ihme_loc_id iso3

gen year=year_start

gen sex_id=.
replace sex_id=1 if sex=="Male"
replace sex_id=2 if sex=="Female"

split age_cat, p( to )
destring age_cat1, replace
destring age_cat2, replace

rename age_cat1 age_start
rename age_cat2 age_end


            preserve
				qui keep if age_start>=65
				collapse (sum) tb, by(iso3 year sex_id)
				qui gen age=65
				tempfile tmp_65
				save `tmp_65', replace
			restore
		// create under 5 age group
			preserve
			    qui keep if age_start==0
				qui gen age=0
				drop age_*
				tempfile tmp_0
				save `tmp_0', replace
			restore
		// create custome age groups 5-15, 15-25, ..., 55-65	
			drop if age_start==0
			drop if age_start>=65
			/* replace age_group_name ="80 to 99" if age_group_name=="80 plus" */
				forvalues i=5(10)55 {
				preserve
					local k=`i'+5
					keep if age_start>=`i' & age_start<=`k'
					collapse (sum) tb, by(iso3 year sex_id)
					gen age=`i'
					tempfile tmp_`i'
					save `tmp_`i'', replace 
				restore
			}
			//append the files
			use "`tmp_0'", clear
			forvalues i=5(10)55 {
				qui append using "`tmp_`i''"
			}
			
			append using "`tmp_65'"
			
			rename tb inc_cases
			
tempfile BRA
save `BRA', replace


// NZL subnationals

import excel using "FILEPATH\incidence_custom4to5star_WHOadjusted_raw_completeGBD2017.xlsx", firstrow clear

keep ihme_loc_id year_start age_start sex cases

rename ihme_loc_id iso3 
rename year_start year 
rename age_start age 
gen sex_id=1
replace sex_id=2 if sex=="Female"
rename cases inc_cases

keep if regexm(iso3,"NZL_")

preserve
keep if age<=1
collapse(sum) inc_cases, by(iso3 year) fast
gen age=0
tempfile age_0
save `age_0', replace
restore

preserve
qui keep if age>=65
collapse (sum) inc_cases, by(iso3 year sex_id)
qui gen age=65
tempfile age_65
save `age_65', replace
restore
			
// create custome age groups 5-15, 15-25, ..., 55-65	
			drop if age<=1
			drop if age>=65
			
				forvalues i=5(10)55 {
				preserve
					local k=`i'+5
					keep if age>=`i' & age<=`k'
					collapse (sum) inc_cases, by(iso3 year sex_id)
					gen age=`i'
					tempfile tmp_`i'
					save `tmp_`i'', replace 
				restore
			}

// append the files
			use "`age_0'", clear
			forvalues i=5(10)55 {
				qui append using "`tmp_`i''"
			}
			
			append using "`age_65'"
			
						
tempfile NZL
save `NZL', replace



use `inc', clear
append using `BRA'
append using `TWN'
append using `NZL'
tempfile inc_all
save `inc_all', replace

use "FILEPATH/all_tb_death_MI_input.dta", clear
keep location_id year_id age_group_id sex_id mean_death
rename mean_death all_tb_death
merge m:1 age_group_id using "FILEPATH\age_group_ids.dta", keep(3)nogen
merge m:1 location_id using "FILEPATH\location_22.dta", keepusing(ihme_loc_id region_name) keep(3)nogen
rename ihme_loc_id iso3
gen BRA_sub=regexm(iso3,"BRA_")
gen NZL_sub=regexm(iso3,"NZL_")
merge m:1 iso3 using `star', nogen
keep if star_4_5==1 | BRA_sub==1 | NZL_sub==1
drop if age_group_id==22

            preserve
				qui keep if age_group_id>=18
				collapse (sum) all_tb_death, by(iso3 year_id sex sex_id)
				qui gen age=65
				tempfile tmp_65
				save `tmp_65', replace
			restore
		// create under 5 age group
			preserve
			    qui keep if age_group_id<=5
				collapse (sum) all_tb_death, by(iso3 year_id sex sex_id)
				qui gen age=0
				tempfile tmp_0
				save `tmp_0', replace
			restore
		// create custome age groups 5-15, 15-25, ..., 55-65	
			drop if age_group_id<=5
			drop if age_group_id>=18
			split age_group_name,p( to )
			gen age=age_group_name1
			destring age, replace
			forvalues i=5(10)55 {
				preserve
					local k=`i'+5
					keep if age>=`i' & age<=`k'
					collapse (sum) all_tb_death, by(iso3 year sex sex_id)
					gen age=`i'
					tempfile tmp_`i'
					save `tmp_`i'', replace 
				restore
			}
			//append the files
			use "`tmp_0'", clear
			forvalues i=5(10)55 {
				qui append using "`tmp_`i''"
			}
			
			append using "`tmp_65'"
						
			qui drop if year<1990
			rename year_id year
			

			merge 1:1 iso3 year age sex_id using `inc_all', keep(3)nogen
			merge m:1 iso3 using `high_sdi', nogen
					
			gen CFR=all_tb_death/inc_cases
			
		 	drop if inc_cases<20
			
			drop year_start year_end sex location_id location_name
			
			save "FILEPATH\MI_input_raw.dta", replace
			
						
			drop if CFR==.
			
			** compute median CFR by age in high SDI locations 
	
			keep if high_sdi==1 
				
			preserve
			
			keep if sex_id==1
			
			tabstat CFR, by(age) stat(median) nototal
			
			restore 
			
			keep if sex_id==2
			
			tabstat CFR, by(age) stat(median) nototal
		 
						
			
			use "FILEPATH\MI_input_raw.dta", clear
			
			drop if CFR==.
			
	        gen age_start=age
			
			gen age_end=age_start+9
			
			replace age_end=4 if age_start==0
			
			replace age_end=100 if age_start==65
		
			order iso3 year age_start age_end sex* all_tb_death inc_cases CFR
			
		    drop if CFR>2 

	
		  // determine the 5th, 95th, and 98th percentile
		  
		  sort age_start sex
          by age_start sex: egen p5 = pctile(CFR), p(5)
		  by age_start sex: egen p95 = pctile(CFR), p(95)
		  by age_start sex: egen p98 = pctile(CFR), p(98)
		  
		 	  
		  sum p98
		  		  
		  // cap and rescale CFR using the 98th percentile max value (will multiply the predicted MI ratios by the same value later) 
		  
		  gen CFR_new=CFR 
		  
		  replace CFR_new=1.139966 if CFR_new>1.139966
		  
		  replace CFR_new=CFR_new/1.139966
** ***************************************************************************************************************************************************


gen year_id=year

// transform to logit
gen logit_prop=logit(CFR_new)

gen ihme_loc_id=iso3

merge m:1 ihme_loc_id using "FILEPATH\location_22.dta", keepusing(location_id location_name) keep(3)nogen

tempfile cfr_countries
save `cfr_countries', replace

import excel using "FILEPATH\Bangalore.xlsx", firstrow clear
// apply the same rescale factor
gen CFR_new=CFR/1.139966
gen logit_prop=logit(CFR_new)

tempfile cfr_bangalore
save `cfr_bangalore', replace

use `cfr_countries', clear
append using `cfr_bangalore'

replace year=year_id if year==.

			
save "FILEPATH\logit_`date'.dta", replace

******************************************************************************************************************************************************


// Get custom age groups
use "FILEPATH\pop_custom_age.dta", clear
gen ihme_loc_id=iso3
merge m:1 ihme_loc_id using "FILEPATH\location_22.dta", keepusing(location_id) keep(3)nogen
rename year year_id
keep location_id iso3 year_id age sex_id

preserve
keep if year_id<2000
replace year_id=year_id-10
tempfile tmp_1980
save `tmp_1980', replace
restore

append using `tmp_1980'

sort location_id year_id age sex_id

tempfile pop_custom
save `pop_custom', replace


// Get covariates
use "FILEPATH\haqi.dta", clear
rename mean_value haq
keep location_id year_id haq

// merge on custom age groups
merge 1:m location_id year_id using `pop_custom', keep(3)nogen

// append template for year 1960 so that Bangalore data won't be dropped

append using  "FILEPATH\template_1960.dta"

tempfile haq
save `haq', replace



use "FILEPATH\logit_`date'.dta", clear

drop haq

// merge on covariates
merge m:1 location_id year_id age sex_id using `haq'


// create age sex dummies

tab age, gen(age_new)

rename age_new1 age0to4
rename age_new2 age5to14
rename age_new3 age15to24
rename age_new4 age25to34
rename age_new5 age35to44
rename age_new6 age45to54
rename age_new7 age55to64
rename age_new8 age65plus

replace haq=0 if year==1960 

gen female=0

replace female=1 if sex_id==2

save "FILEPATH\MI_input_`date'.dta", replace
