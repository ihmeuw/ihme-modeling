
** MI ratio regression


// get the SDI covariate
insheet using "FILEPATH", comma names clear
rename mean_value sdi
keep location_id location_name year_id sdi
tempfile sdi
save `sdi', replace

// bring in 4-5 star locations
insheet using "FILEPATH", comma names clear
gen star_4_5=1
tempfile star
save `star', replace

// bring in high SDI locations
insheet using "FILEPATH", comma names clear
merge m:1 location_id using "FILEPATH", keepusing(ihme_loc_id) keep(3)nogen
split ihme_loc_id, p(_)
rename ihme_loc_id1 iso3
duplicates drop iso3, force
keep iso3
gen high_sdi=1
tempfile high_sdi
save `high_sdi', replace

// bring in notification data for 4-5 star countries
use "FILEPATH", clear
keep pop iso3 year age sex CN_bact_xb pop
gen sex_id=.
replace sex_id=1 if sex=="Male"
replace sex_id=2 if sex=="Female"

rename CN_bact_xb inc_cases

drop if year<2006

tempfile inc
save `inc', replace

// prep BRA notification data

use "FILEPATH", clear

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

    // prep for the 65+ age group
	preserve
		qui keep if age_start>=65
		collapse (sum) tb, by(iso3 location_name year sex sex_id)
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
		forvalues i=5(10)55 {
				preserve
					local k=`i'+5
					keep if age_start>=`i' & age_start<=`k'
					collapse (sum) tb, by(iso3 location_name year sex sex_id)
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

// append files
use `inc', clear
append using `BRA'
tempfile inc_all
save `inc_all', replace

// prep TB deaths

use "FILEPATH", clear
merge m:1 age_group_id using "FILEPATH", keep(3)nogen
merge m:1 location_id using "FILEPATH", keepusing(ihme_loc_id region_name) keep(3)nogen
rename ihme_loc_id iso3
gen BRA_sub=regexm(iso3,"BRA_")
merge m:1 iso3 using `star', nogen
keep if star_4_5==1 | BRA_sub==1
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
			/* replace age_group_name ="80 to 99" if age_group_name=="80 plus" */
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
			
			/*					
			keep if high_sdi==1
				
			preserve
			
			keep if sex=="Male"
			
			tabstat CFR, by(age) stat(median) nototal
			
			restore 
			
			keep if sex=="Female"
			
			tabstat CFR, by(age) stat(median) nototal
			*/
			
			/* sum CFR, detail 

            gen RR_median=`r(p50)'  */
			
			
	       gen age_start=age
			
			gen age_end=age_start+9
			
			replace age_end=4 if age_start==0
			
			replace age_end=100 if age_start==65
			
			// drop values lower than the median value for high SDI locations
			
		   replace high_sdi=0 if high_sdi==.
			
		   drop if CFR<.0061683 & high_sdi==0 & age==0 & sex=="Male"
		   drop if CFR<.0102126 & high_sdi==0 & age==5 & sex=="Male"
		   drop if CFR<.0039422 & high_sdi==0 & age==15 & sex=="Male"
		   drop if CFR<.0125591 & high_sdi==0 & age==25 & sex=="Male"
		   drop if CFR<.0392145 & high_sdi==0 & age==35 & sex=="Male"
		   drop if CFR<.0704044 & high_sdi==0 & age==45 & sex=="Male"
		   drop if CFR<.110968 & high_sdi==0 & age==55 & sex=="Male"
		   drop if CFR<.306967 & high_sdi==0 & age==65 & sex=="Male"
				  
		   drop if CFR<.0097341 & high_sdi==0 & age==0 & sex=="Female"
		   drop if CFR<.009555 & high_sdi==0 & age==5 & sex=="Female"
		   drop if CFR<.0041473 & high_sdi==0 & age==15 & sex=="Female"
		   drop if CFR<.0083984 & high_sdi==0 & age==25 & sex=="Female"
		   drop if CFR<.0221097 & high_sdi==0 & age==35 & sex=="Female"
		   drop if CFR<.0471237 & high_sdi==0 & age==45 & sex=="Female"
		   drop if CFR<.0839199 & high_sdi==0 & age==55 & sex=="Female"
		   drop if CFR<.3229932 & high_sdi==0 & age==65 & sex=="Female"
			
		   order iso3 year age_start age_end sex* pop all_tb_death inc_cases CFR
			
	       // exclude if CFR>2
		   
		    drop if CFR>2

		  // determine the 5th and 95th percentile
		  
		  sort age_start sex
          by age_start sex: egen p5 = pctile(CFR), p(5)
		  by age_start sex: egen p95 = pctile(CFR), p(95)
		  
		  gen CFR_new=CFR
		  
		  // cap and rescale CFR (will scale back to original value later)
		  replace CFR_new=1.167345 if CFR_new>1.167345  
		  
		  replace CFR_new=CFR_new/1.167345
		  
** ***************************************************************************************************************************************************

// drop unnecessary variables

drop year_*	location_*	  

gen year_id=year

// compute logit CFR
gen logit_prop=logit(CFR_new)

  
gen ihme_loc_id=iso3

merge m:1 ihme_loc_id using "FILEPATH", keepusing(location_id) keep(3)nogen

merge m:1 location_id year_id using `haq', keep(3)nogen 

merge m:1 location_id year_id using `sdi', keep(3)nogen 
			
save "logit.dta", replace

**************************************************************************************************************************************
// regression

// bring in data from the Bangalore study

import excel using "FILEPATH", firstrow clear
tempfile bangalore
save `bangalore', replace

// merge the files
use "logit.dta", clear
drop sdi
merge m:1 ihme_loc_id using "FILEPATH", keepusing(location_id) keep(3)nogen	
append using `bangalore'
tempfile logit
save `logit', replace

use `sdi', clear
merge 1:m location_id year_id using "FILEPATH", keepusing(age_group_id sex_id) keep(3)nogen
keep if inlist(age_group_id,5,6,8,10,12,14,16,18)
gen age=.
replace age=0 if age_group_id==5
replace age=5 if age_group_id==6
replace age=15 if age_group_id==8
replace age=25 if age_group_id==10
replace age=35 if age_group_id==12
replace age=45 if age_group_id==14
replace age=55 if age_group_id==16
replace age=65 if age_group_id==18

tempfile sdi
save `sdi', replace

merge 1:m location_id year_id age sex_id using `logit', keepusing(logit_prop CFR_new) nogen 
 
gen female=0
replace female=1 if sex_id==2

tab age, gen(age_new)

merge m:1 location_id using "FILEPATH", keepusing(ihme_loc_id) keep(3)nogen
rename ihme_loc_id iso3

replace sdi=0 if year==1970 | year==1971

// drop aggregate locations
    
    drop if inlist(location_id,1, 4, 5, 9, 21, 31, 32, 42, 56, 64, 65, 70, 73, 96, 100, 103, 104, 120, 124, 134, 137, 138, 158, 159, 166, 167, 174, 192, 199)
    
	cap log close 	
	log using "FILEPATH", replace

		//  regression 
		
		regress logit_prop age_new2 age_new3 age_new4 age_new5 age_new6 age_new7 age_new8 female sdi

    // predict for all locations
    predict pred_logit_ep_prop
    predict stdp, stdp
    gen pred_prop= invlogit(pred_logit_ep_prop)
	// these are rescaled CFRs so multiply them by 1.167345 to scale back to the original values
    gen rescale_prop=pred_prop*1.167345

    save "FILEPATH", replace

     keep location_id location_name year_id age_group_id sex_id sdi age rescale_prop

     sort location_id year_id age_group_id sex_id

     rename rescale_prop cfr_pred

  tempfile cfr
  save `cfr', replace

// bring in all TB deaths
use "FILEPATH", clear
keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
// merge on age group ids
merge m:1 age_group_id using "FILEPATH", keep(3)nogen
// merge on location ids
merge m:1 location_id using "FILEPATH", keepusing(ihme_loc_id region_name) keep(3)nogen
rename ihme_loc_id iso3
drop if age_group_id==22

preserve
				qui keep if age_group_id>=18
				collapse (sum) all_tb_death, by(location_id iso3 year_id sex sex_id)
				qui gen age=65
				tempfile tmp_65
				save `tmp_65', replace
			restore
		// create under 5 age group
			preserve
			    qui keep if age_group_id<=5
				collapse (sum) all_tb_death, by(location_id iso3 year_id sex sex_id)
				qui gen age=0
				tempfile tmp_0
				save `tmp_0', replace
			restore
		// create custome age groups 5-15, 15-25, ..., 55-65	
			drop if age_group_id<=5
			drop if age_group_id>=18
			/* replace age_group_name ="80 to 99" if age_group_name=="80 plus" */
			split age_group_name,p( to )
			gen age=age_group_name1
			destring age, replace
			forvalues i=5(10)55 {
				preserve
					local k=`i'+5
					keep if age>=`i' & age<=`k'
					collapse (sum) all_tb_death, by(location_id iso3 year sex sex_id)
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
			/* rename year_id year */

tempfile death
save `death', replace

// bring in prevalence data
use "FILEPATH", clear
keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
keep location_id year_id age_group_id sex_id mean
// merge on age group ids
merge m:1 age_group_id using "FILEPATH", keep(3)nogen
// merge on population
merge m:1 location_id year_id age_group_id sex_id using "FILEPATH", keepusing(population) keep(3)nogen
gen cases=mean*population
drop if age_group_id==22
           preserve
				qui keep if age_group_id>=18
				collapse (sum) cases population, by(location_id year_id sex_id)
				qui gen age=65
				tempfile tmp_65
				save `tmp_65', replace
			restore
		// create under 5 age group
			preserve
			    qui keep if age_group_id<=5
				collapse (sum) cases population, by(location_id year_id sex sex_id)
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
					collapse (sum) cases population, by(location_id year sex sex_id)
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
			/* rename year_id year */

gen prev= cases/population
tempfile prev
save `prev', replace

// Adjust for HIV
// bring in HIVTB prevalence
use "FILEPATH", clear
keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
keep location_id year_id age_group_id sex_id mean
// merge on age group ids
merge m:1 age_group_id using "FILEPATH", keep(3)nogen
// merge on population
merge m:1 location_id year_id age_group_id sex_id using "FILEPATH", keepusing(population) keep(3)nogen
gen cases=mean*population
drop if age_group_id==22
preserve
				qui keep if age_group_id>=18
				collapse (sum) cases population, by(location_id year_id sex_id)
				qui gen age=65
				tempfile tmp_65
				save `tmp_65', replace
			restore
		// create under 5 age group
			preserve
			    qui keep if age_group_id<=5
				collapse (sum) cases population, by(location_id year_id sex sex_id)
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
					collapse (sum) cases population, by(location_id year sex sex_id)
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
			/* rename year_id year */

gen hivtb_prev= cases/population

merge 1:1 location_id year_id age sex_id using `prev', keepusing(prev) keep(3)nogen
gen hiv_prop=hivtb_prev/prev

gen RR_median=1.726063

gen hiv_rr_cyas=hiv_prop*RR_median+(1-hiv_prop)*1

tempfile hiv
save `hiv', replace

// bring in EMR data
use "FILEPATH", clear
keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
keep location_id year_id age_group_id sex_id mean
merge m:1 age_group_id using "FILEPATH", keep(3)nogen
merge m:1 location_id year_id age_group_id sex_id using "FILEPATH", keepusing(population) keep(3)nogen
gen cases=mean*population
drop if age_group_id==22
preserve
				qui keep if age_group_id>=18
				collapse (sum) cases population, by(location_id year_id sex_id)
				qui gen age=65
				tempfile tmp_65
				save `tmp_65', replace
			restore
		// create under 5 age group
			preserve
			    qui keep if age_group_id<=5
				collapse (sum) cases population, by(location_id year_id sex sex_id)
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
					collapse (sum) cases population, by(location_id year sex sex_id)
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
			

gen emr= cases/population
tempfile emr
save `emr', replace

use `death', clear
merge 1:1 location_id year_id age sex_id using `prev', keep(3)nogen
merge 1:1 location_id year_id age sex_id using `emr', keep(3)nogen
merge m:1 location_id year_id age sex_id using `cfr', keep(3)nogen
merge m:1 location_id year_id age sex_id using `hiv', keep(3)nogen

gen cfr_pred_hiv=cfr_pred*hiv_rr_cyas

gen csmr=all_tb_death/population

gen inc_cases=all_tb_death/cfr_pred

gen inc_cases_hiv=all_tb_death/cfr_pred_hiv

// format
gen sex=""
replace sex="Male" if sex_id==1
replace sex="Female" if sex_id==2

gen age_start=age
			
			gen age_end=age_start+9
			
			replace age_end=4 if age_start==0
			
			replace age_end=100 if age_start==65

gen inc=inc_cases/population

gen inc_hiv=inc_cases_hiv/population

gen duration=prev/inc

gen duration_hiv=prev/inc_hiv

order location_id location_name iso3 year_id sex* age_start age_end sdi all_tb_death cfr_pred cfr_pred_hiv inc_cases inc_cases_hiv duration duration_hiv

save "FILEPATH", replace
