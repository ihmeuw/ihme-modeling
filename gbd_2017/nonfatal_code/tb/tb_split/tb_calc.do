** Description: (1) Calculate population level incidence and prevalence (i.e., multiplying DisMod TB estimates by LTBI prevalence)
**              (2) split #1 into HIV-TB and TB no-HIV
**              

		// Settings
			// Clear memory and set memory and variable limits
				clear all
				
			// Set to run all selected code without pausing
				set more off

			// Set graph output color scheme
				set scheme s1color

			// Define J drive (data) for cluster (UNIX) and Windows (Windows)
				if c(os) == "Unix" {
					global prefix "/ADDRESS/"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "/ADDRESS/"
				}
			
			// Close any open log file
				cap log close
				
			
		    // locals 
				local acause tb
				local model_version_id dismod_332315 /* me_id=9422 (dismod)*/
			   	local ltbi 325868 
				local hiv 326429  
				
				
			// Make folders to store COMO files
		
			capture mkdir "/ADDRESS/"
			capture mkdir "/ADDRESS/"
			capture mkdir "/ADDRESS/"
			capture mkdir "/ADDRESS/"
			capture mkdir "/ADDRESS/"	
			
			capture mkdir "/ADDRESS/"
			capture mkdir "/ADDRESS/"
		    capture mkdir "/ADDRESS/"
			
			capture mkdir "/ADDRESS/"
			capture mkdir "/ADDRESS/"
		    capture mkdir "/ADDRESS/"
			
			capture mkdir "/ADDRESS/"
			capture mkdir "/ADDRESS/"
		    capture mkdir "/ADDRESS/"
			
			capture mkdir "/ADDRESS/"
			capture mkdir "/ADDRESS/"
		    capture mkdir "/ADDRESS/"
			
			capture mkdir "/ADDRESS/"
			capture mkdir "/ADDRESS/"
		    capture mkdir "/ADDRESS/"

			
			capture mkdir "/ADDRESS/"
			capture mkdir "/ADDRESS/"
		    capture mkdir "/ADDRESS/"
			
			capture mkdir "/ADDRESS/"
			capture mkdir "/ADDRESS/"
		    capture mkdir "/ADDRESS/"
			
			capture mkdir "/ADDRESS/"
			capture mkdir "/ADDRESS/"
		    capture mkdir "/ADDRESS/"


** *************************************************************************************************************************************************************
** (1) Compute population level incidence and prevalence
** *************************************************************************************************************************************************************

// pull TB draws


adopath + "/ADDRESS/"

get_draws, gbd_id_type(modelable_entity_id) gbd_id(9422) measure_id(5 6) num_workers(15) source(epi) clear

// drop aggregate locations
drop if inlist(location_id,1, 4, 5, 9, 21, 31, 32, 42, 56, 64, 65, 70, 73, 96, 100, 103, 104, 120, 124, 134, 137, 138, 158, 159, 166, 167, 174, 192, 199)

save /FILEPATH/, replace

/*
adopath + "/ADDRESS/"
get_draws, gbd_id_type(modelable_entity_id) gbd_id(10352) version_id(325868) source(epi) clear
// drop aggregate locations
drop if inlist(location_id,1, 4, 5, 9, 21, 31, 32, 42, 56, 64, 65, 70, 73, 96, 100, 103, 104, 120, 124, 134, 137, 138, 158, 159, 166, 167, 174, 192, 199)
drop measure*
// rename draws
forvalues i = 0/999 {
			  rename draw_`i' ltbi_`i'
			}

save /FILEPATH/, replace		
*/

use "/FILEPATH/", clear


// drop countries with subnationals 
drop if inlist(location_id,6, 62, 67, 72, 90, 93, 95, 102, 130, 135, 142, 163, 179, 180, 196)

// merge on LTBI

merge m:1 location_id year_id age_group_id sex_id using "/FILEPATH/", keep(3)nogen

forvalues i=0/999 {
			di in red "draw `i'"
			replace draw_`i'=draw_`i'*ltbi_`i'
			}
drop ltbi_*			
tempfile tb_all
save `tb_all', replace

save "/FILEPATH/", replace


keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235

replace modelable_entity_id=9806

duplicates drop location_id year_id age_group_id sex_id measure_id, force

drop model_version_id

order modelable_entity_id measure_id location_id year_id age_group_id sex_id

sort measure_id location_id year_id age_group_id sex_id

preserve

keep if measure_id==5

tempfile prev
save `prev', replace

restore

keep if measure_id==6

tempfile inc
save `inc', replace

// save results 

use `prev',clear

levelsof(location_id), local(ids) clean

foreach location_id of local ids {
		
					qui outsheet if location_id==`location_id' using "/FILEPATH/", comma replace
				
			}
		

		
use `inc',clear

levelsof(location_id), local(ids) clean

foreach location_id of local ids {
					qui outsheet if location_id==`location_id' using "/FILEPATH/", comma replace
			
			}
	

run "/FILEPATH/"

save_results_epi, input_dir("/ADDRESS/") input_file_pattern({location_id}_{measure_id}.csv) modelable_entity_id(9806) mark_best("True") description("`model_version_id'") measure_id(5 6) db_env("prod") clear 


** *************************************************************************************************************************************************************
** (2) Calculate HIV-TB 
** *************************************************************************************************************************************************************	
/*
adopath + "/ADDRESS/"
get_draws, gbd_id_type(modelable_entity_id) gbd_id(9806) measure_id(5 6) num_workers(15) source(epi) clear
keep measure_id location_id year_id age_group_id sex_id draw_*
save "/FILEPATH/", replace 
*/


use "/FILEPATH/", clear

keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235

preserve

keep if measure_id==5

duplicates drop location_id year_id age_group_id sex_id measure_id, force

tempfile prev
save `prev', replace

restore

keep if measure_id==6

duplicates drop location_id year_id age_group_id sex_id measure_id, force

tempfile inc
save `inc', replace

// get HIV prev age pattern


clear all
adopath + "/ADDRESS/"
get_model_results, gbd_team("epi") gbd_id(9368) location_set_version_id(319) clear

save /FILEPATH/, replace


use "/FILEPATH/", clear
drop model_version_id
keep if measure_id==5
keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
rename mean rate
drop lower upper
tempfile age_pattern
save `age_pattern', replace


// get population
	  
		clear all
		adopath + "/ADDRESS/"
		get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") location_set_version_id(319) clear
		rename population mean_pop
		tempfile pop_all
		save `pop_all', replace
		
		
		// keep sex-specific pop
		use `pop_all', clear
		drop if year_id<1980
		drop if location_id==1
		drop if sex_id==3
		tempfile tmp_pop
		save `tmp_pop', replace


// run ado file for fast collapse

adopath+ "/ADDRESS/"


** ******************************** calculate HIV-TB prevalence *********************************************************************************************

// collapse draws by location_id and year_id

use `prev', clear
merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen
forvalues i=0/999 {
			di in red "draw `i'"
			replace draw_`i'=draw_`i'*mean_pop
			}

			tempfile prev_cases
			save `prev_cases', replace

keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235

fastcollapse draw_*, type(sum) by(location_id year_id) 

** merge on the proportion data
merge 1:1 location_id year_id using "/FILEPATH/", keepusing(mean_prop) keep(3)nogen
		
	** loop through draws and compute HIV-TB... 
		forvalues i=0/999 {
			di in red "draw `i'"
			gen tbhiv_d`i'=mean_prop*draw_`i'
			drop draw_`i' 
		}
		

// prep for age split

merge 1:m location_id year_id using `tmp_pop', keep(1 3) nogen
merge m:1 location_id year_id age_group_id sex_id using `age_pattern', keep(3)nogen

rename mean_pop sub_pop
gen rate_sub_pop=rate*sub_pop

preserve
collapse (sum) rate_sub_pop, by(location_id year_id) fast
rename rate_sub_pop sum_rate_sub_pop
tempfile sum
save `sum', replace

restore
merge m:1 location_id year_id using `sum', keep(3)nogen

forvalues i=0/999 {
			di in red "draw `i'"
			gen draw_`i'=rate_sub_pop*(tbhiv_d`i'/sum_rate_sub_pop)
			drop tbhiv_d`i' 
		}

keep location_id year_id age_group_id sex_id draw_*

tempfile hivtb_prev_cyas
save `hivtb_prev_cyas', replace


// cap hivtb cases if hivtb/tb >90% of TB all forms

// rename tb draws
use `prev_cases', clear
// rename draws
forvalues i = 0/999 {
			  rename draw_`i' tb_`i'
			}

// merge the tb-all-forms and hivtb files
			
			merge 1:1 location_id year_id age_group_id sex using `hivtb_prev_cyas', keep(3) nogen 

// loop through draws and adjust them... 
		forvalues i=0/999 {
			gen frac_`i'=draw_`i'/tb_`i'
			replace draw_`i'=tb_`i'*0.9 if frac_`i'>0.9 & frac_`i' !=.
			replace draw_`i'=0 if draw_`i'==.
			}
drop tb_* frac_* m*
tempfile hivtb_prev_capped
save `hivtb_prev_capped', replace


// merge on pop again to calculate prevalence
merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen
forvalues i=0/999 {
			di in red "draw `i'"
			replace draw_`i'=draw_`i'/mean_pop
			}
			
// Locations where the prevalence of HIV is zero (location_ids 161 and 186 for 1990) have missing draws, so replace them with zero
foreach a of varlist draw_0-draw_999 {
	replace `a'=0 if `a'==.
	}


*****************************************************************
gen modelable_entity_id=1176
gen measure_id=5
tempfile hivtb_cyas_prev_capped
save `hivtb_cyas_prev_capped', replace
save /FILEPATH/, replace


** ******************************** calculate HIV-TB incidence *********************************************************************************************


// collapse draws by location_id and year_id

use `inc', clear
merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen
forvalues i=0/999 {
			di in red "draw `i'"
			replace draw_`i'=draw_`i'*mean_pop
			}

			tempfile inc_cases
			save `inc_cases', replace

keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235

fastcollapse draw_*, type(sum) by(location_id year_id) 

** merge on the fraction data
merge 1:1 location_id year_id using "/FILEPATH/", keepusing(mean_prop) keep(3)nogen
		
	** loop through draws and calculate cases... 
		forvalues i=0/999 {
			di in red "draw `i'"
			gen tbhiv_d`i'=mean_prop*draw_`i'
			drop draw_`i' 
		}


// prep for age split

merge 1:m location_id year_id using `tmp_pop', keep(1 3) nogen
merge m:1 location_id year_id age_group_id sex_id using `age_pattern', keep(3)nogen

rename mean_pop sub_pop
gen rate_sub_pop=rate*sub_pop

preserve
collapse (sum) rate_sub_pop, by(location_id year_id) fast
rename rate_sub_pop sum_rate_sub_pop
tempfile sum
save `sum', replace

restore
merge m:1 location_id year_id using `sum', keep(3)nogen

forvalues i=0/999 {
			di in red "draw `i'"
			gen draw_`i'=rate_sub_pop*(tbhiv_d`i'/sum_rate_sub_pop)
			drop tbhiv_d`i' 
		}

keep location_id year_id age_group_id sex_id draw_*

tempfile hivtb_inc_cyas
save `hivtb_inc_cyas', replace


// Cap hivtb cases if hivtb/tb >90% of TB all forms

// rename tb draws
use `inc_cases', clear
// rename draws
forvalues i = 0/999 {
			  rename draw_`i' tb_`i'
			}



// merge the tb-all-forms and hivtb files
		
			merge 1:1 location_id year_id age_group_id sex using `hivtb_inc_cyas', keep(3) nogen 

// loop through draws and adjust them... 
		forvalues i=0/999 {
			gen frac_`i'=draw_`i'/tb_`i'
			replace draw_`i'=tb_`i'*0.9 if frac_`i'>0.9 & frac_`i' !=.
			replace draw_`i'=0 if draw_`i'==.
			}
drop tb_* frac_* m*
tempfile hivtb_inc_capped
save `hivtb_inc_capped', replace


// merge on pop again to calculate incidence
merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen
forvalues i=0/999 {
			di in red "draw `i'"
			replace draw_`i'=draw_`i'/mean_pop
			}
			
// Locations where the prevalence of HIV is zero (location_ids 161 and 186 for 1990) have missing draws, so replace them with zero
foreach a of varlist draw_0-draw_999 {
	replace `a'=0 if `a'==.
	}

*****************************************************************
gen modelable_entity_id=1176
gen measure_id=6
tempfile hivtb_cyas_inc_capped
save `hivtb_cyas_inc_capped', replace
save /FILEPATH/, replace



** *************************************************************************************************************************************************************
** (3) Calculate TB no-HIV
** *************************************************************************************************************************************************************	

clear all
		adopath + "/ADDRESS/"
		get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") location_set_version_id(319) clear
		rename population mean_pop
		tempfile pop_all
		save `pop_all', replace
		
// Bring in HIVTB

// hivtb inc
use /FILEPATH/, clear
keep location_id year_id age_group_id sex_id draw_* mean_pop
// rename draws
forvalues i = 0/999 {
			  rename draw_`i' hivtb_`i'  
			  replace hivtb_`i'=hivtb_`i'*mean_pop
			}
tempfile hivtb_inc
save `hivtb_inc', replace

// hivtb prev

use /FILEPATH/, clear
keep location_id year_id age_group_id sex_id draw_* mean_pop
// rename draws
forvalues i = 0/999 {
			  rename draw_`i' hivtb_`i'
			  replace hivtb_`i'=hivtb_`i'*mean_pop
			}
tempfile hivtb_prev
save `hivtb_prev', replace

// bring in TB all forms
** calculate TB no-HIV incidence
use "/FILEPATH/", clear
keep if measure_id==6
duplicates drop location_id year_id age_group_id sex_id measure_id, force
merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

forvalues i=0/999 {
			di in red "draw `i'"
			replace draw_`i'=draw_`i'*mean_pop
			}


merge 1:1 location_id year_id age_group_id sex using `hivtb_inc', keep(3) nogen
// loop through draws and subtract hiv_tb from hiv 
		forvalues i=0/999 {
			replace draw_`i'=draw_`i'-hivtb_`i'
			replace draw_`i'=draw_`i'/mean_pop
			}

			drop mean_pop
tempfile tb_noHIV_inc
save `tb_noHIV_inc', replace

** calculate TB no-HIV prevalence
use "/FILEPATH/", clear
keep if measure_id==5
duplicates drop location_id year_id age_group_id sex_id measure_id, force

merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

forvalues i=0/999 {
			di in red "draw `i'"
			replace draw_`i'=draw_`i'*mean_pop
			}


merge 1:1 location_id year_id age_group_id sex using `hivtb_prev', keep(3) nogen
// loop through draws and subtract hiv_tb from hiv 
		forvalues i=0/999 {
			replace draw_`i'=draw_`i'-hivtb_`i'
			replace draw_`i'=draw_`i'/mean_pop
			}
drop mean_pop
tempfile tb_noHIV_prev
save `tb_noHIV_prev', replace

append using `tb_noHIV_inc'

keep measure_id location_id year_id age_group_id sex_id draw_*

gen modelable_entity_id=9969

save "/FILEPATH/", replace 

