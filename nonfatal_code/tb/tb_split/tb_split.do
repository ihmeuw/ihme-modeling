** Description: (1) pull TB epi draws among the infected population and calculate population level incidence and prevalence
**              (2) split #1 into HIV-TB and TB no-HIV
**              (3) use TB no-HIV from #3 and MDR proportions (among HIV negatives) to calculate MDR-TB no-HIV cases, and drug sensitive TB no-HIV cases
**              (4) calculate TB no-HIV XDR, and MDR without XDR

		// Settings
			// Clear memory and set memory and variable limits
				clear all
				set mem 5G
				set maxvar 32000

			// Set to run all selected code without pausing
				set more off

			// Set graph output color scheme
				set scheme s1color

			// Define J drive (data) for cluster (UNIX) and Windows (Windows)
				if c(os) == "Unix" {
					global prefix "ADDRESS"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "ADDRESS"
				}
			
			// Close any open log file
				cap log close
				
			
		    // locals 
				local acause tb
				local model_version_id dismod_189863
			    /* local data_rich 107582 */
				local ltbi 175397
				local hiv 189884
				
				
			// Make folders to store COMO files
		
			capture mkdir "ADDRESS"
			capture mkdir "ADDRESS"	
			capture mkdir "ADDRESS"
			capture mkdir "ADDRESS"	
			
			capture mkdir "ADDRESS"
			capture mkdir "ADDRESS"	
			capture mkdir "ADDRESS"
			
			capture mkdir "ADDRESS"
			capture mkdir "ADDRESS"	
			capture mkdir "ADDRESS"
			
			capture mkdir "ADDRESS"
			capture mkdir "ADDRESS"	
			capture mkdir "ADDRESS"
			
			capture mkdir "ADDRESS"
			capture mkdir "ADDRESS"	
			capture mkdir "ADDRESS"
			
			capture mkdir "ADDRESS"
			capture mkdir "ADDRESS"	
			capture mkdir "ADDRESS"
			
			capture mkdir "ADDRESS"
			capture mkdir "ADDRESS"	
			capture mkdir "ADDRESS"
			
			capture mkdir "ADDRESS"
			capture mkdir "ADDRESS"	
			capture mkdir "ADDRESS"
			
			capture mkdir "ADDRESS"
			capture mkdir "ADDRESS"	
			capture mkdir "ADDRESS"


** *************************************************************************************************************************************************************
** (1) Calculate population based TB incidence and prevalence
** *************************************************************************************************************************************************************


// pull TB DisMod draws

adopath + "ADDRESS"
get_draws, gbd_id_field(modelable_entity_id) gbd_id(9422) measure_ids(5 6) model_version_id(189863) source(epi) clear
save "FILEPATH", replace

// pull LTBI draws

adopath + "ADDRESS"
get_draws, gbd_id_field(modelable_entity_id) gbd_id(10352) status(best) source(epi) clear
drop measure*
// rename LTBI draws
forvalues i = 0/999 {
			  rename draw_`i' ltbi_`i'
			}

save "FILEPATH", replace		

// merge TB and LTBI draws

// bring in TB draws
use "FILEPATH", clear
// drop aggregate locations
drop if inlist(location_id,1, 4, 5, 9, 21, 31, 32, 42, 56, 64, 65, 70, 73, 96, 100, 103, 104, 120, 124, 134, 137, 138, 158, 159, 166, 167, 174, 192, 199)

// drop countries with subnationals
drop if inlist(location_id,6, 67, 93, 95, 102, 130, 135, 152, 163, 180, 196)

// merge on LTBI draws

merge m:1 location_id year_id age_group_id sex_id using "FILEPATH", keep(3)nogen

// multiply the DisMod-MR 2.2 outputs by the risk-weighted prevalence of LTBI to get population-level estimates of incidence and prevalence
forvalues i=0/999 {
			di in red "draw `i'"
			replace draw_`i'=draw_`i'*ltbi_`i'
			}
drop ltbi_*			
tempfile tb_all
save `tb_all', replace

save "FILEPATH", replace


keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235

replace modelable_entity_id=9806

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

*****************************************************************************************

// save results (prevalence)

use `prev',clear

levelsof(location_id), local(ids) clean
levelsof(year_id), local(years) clean

global sex_id "1 2"

foreach location_id of local ids {
		foreach year_id of local years {
			foreach sex_id of global sex_id {
					qui outsheet if location_id==`location_id' & year_id==`year_id' & sex_id==`sex_id' using "FILPATH", comma replace
				}
			}
		}
		

		
use `inc',clear

levelsof(location_id), local(ids) clean
levelsof(year_id), local(years) clean

global sex_id "1 2"

foreach location_id of local ids {
		foreach year_id of local years {
			foreach sex_id of global sex_id {
					qui outsheet if location_id==`location_id' & year_id==`year_id' & sex_id==`sex_id' using "FILEPATH", comma replace
				}
			}
		}
		

		

run "FILPATH"
save_results, modelable_entity_id(9806) description(`model_version_id') mark_best(yes) in_dir(ADDRESS) metrics(prevalence incidence)

// save results (incidence)

import excel using "FILEPATH", firstrow clear

keep if measure_id==6

keep if inlist(location_id, 196, 482, 483, 484, 485, 486, 487, 488, 489, 490)

merge m:1 year_id using `adj_factor', keep(3)nogen

forvalues i = 0/999 {
			  replace draw_`i'=draw_`i'*adjustment_factor
			}

gen modelable_entity_id=9806
keep modelable_entity_id location_id year_id age_group_id sex_id draw_*
tempfile ZAF
save `ZAF', replace

levelsof(location_id), local(ids) clean
levelsof(year_id), local(years) clean

global sex_id "1 2"

foreach location_id of local ids {
		foreach year_id of local years {
			foreach sex_id of global sex_id {
					qui outsheet if location_id==`location_id' & year_id==`year_id' & sex_id==`sex_id' using "FILEPATH", comma replace
				}
			}
		}
		

		

run "FILEPATH"
save_results, modelable_entity_id(9806) description(`model_version_id' with ZAF fixes) mark_best(yes) in_dir(ADDRESS) metrics(prevalence incidence)


** *************************************************************************************************************************************************************
** (2) Calculate HIV-TB 
** *************************************************************************************************************************************************************	

// pull TB draws
adopath + "ADDRESS"
get_draws, gbd_id_field(modelable_entity_id) gbd_id(9806) measure_ids(5 6) status(best) source(epi) clear
keep measure_id location_id year_id age_group_id sex_id draw_*
save "FILEPATH", replace 

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
adopath + "ADDRESS"
get_model_results, gbd_team("epi") gbd_id(9368) location_set_id(22) model_version_id(`hiv') clear
keep if measure_id==5
keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
save FILEPATH, replace

drop model_version_id
rename mean rate
drop lower upper
tempfile age_pattern
save `age_pattern', replace



// get population
	  
		clear all
		adopath + "ADDRESS"
		get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") location_set_id(22) clear
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

adopath+ "ADDRESS"

** ******************************** calculate HIV-TB prevalence *********************************************************************************************

// pull hivtb proportions

use "FILEPATH", clear

// Rename draws
			forvalues i = 1/1000 {
				local i1 = `i' - 1
				rename prop_tbhiv_xb_d`i' prop_tbhiv_xb_d`i1'
			}

tempfile hivtb_prop
save `hivtb_prop', replace

		
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

** merge on the fraction data
merge 1:1 location_id year_id using `hivtb_prop', keep(3)nogen
		
	** loop through draws and compute HIVTB deaths 
		forvalues i=0/999 {
			di in red "draw `i'"
			gen tbhiv_d`i'=prop_tbhiv_xb_d`i'*draw_`i'
		}
		drop draw_* prop_tbhiv_xb_d*

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
keep location_id year_id age_group_id sex_id draw_*
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
	
gen modelable_entity_id=1176
tempfile hivtb_cyas_prev_capped
save `hivtb_cyas_prev_capped', replace
save FILEPATH, replace


// format hiv_tb for upload

keep location_id year_id age_group_id sex_id draw_*

gen measure_id=5
	

	// prep for COMO
	levelsof(location_id), local(ids) clean
	levelsof(year_id), local(years) clean

global sex_id "1 2"

foreach location_id of local ids {
		foreach year_id of local years {
			foreach sex_id of global sex_id {
					qui outsheet if location_id==`location_id' & year_id==`year_id' & sex_id==`sex_id' using "FILEPATH", comma replace
				}
			}
		}
		

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
merge 1:1 location_id year_id using `hivtb_prop', keep(3)nogen
		
	** loop through draws and calculate cases... 
		forvalues i=0/999 {
			di in red "draw `i'"
			gen tbhiv_d`i'=prop_tbhiv_xb_d`i'*draw_`i'
		}
        drop draw_*

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
keep location_id year_id age_group_id sex_id draw_*
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
	
gen modelable_entity_id=1176
tempfile hivtb_cyas_inc_capped
save `hivtb_cyas_inc_capped', replace
save FILEPATH, replace


// upload results

use `hivtb_cyas_inc_capped', clear
keep location_id year_id age_group_id sex_id draw_*
gen measure_id=6


	// prep for COMO
	levelsof(location_id), local(ids) clean
	levelsof(year_id), local(years) clean

global sex_id "1 2"

foreach location_id of local ids {
		foreach year_id of local years {
			foreach sex_id of global sex_id {
					qui outsheet if location_id==`location_id' & year_id==`year_id' & sex_id==`sex_id' using "FILEPATH", comma replace
				}
			}
		}
		

run "FILEPATH"
save_results, modelable_entity_id(1176) description(`acause' `model_version_id') mark_best(yes) in_dir(ADDRESS) metrics(prevalence incidence)


** *************************************************************************************************************************************************************
** (3) Calculate TB no-HIV
** *************************************************************************************************************************************************************	

clear all
		adopath + "ADDRESS"
		get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") location_set_id(22) clear
		rename population mean_pop
		tempfile pop_all
		save `pop_all', replace
		
// Bring in HIVTB

// hivtb inc
use FILEPATH, clear
keep location_id year_id age_group_id sex_id draw_* mean_pop
// rename draws
forvalues i = 0/999 {
			  rename draw_`i' hivtb_`i'  
			  replace hivtb_`i'=hivtb_`i'*mean_pop
			}
tempfile hivtb_inc
save `hivtb_inc', replace

// hivtb prev

use FILEPATH, clear
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
use "FILEPATH", clear
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
use "FILEPATH", clear
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

save "FILEPATH", replace 


preserve

keep if measure_id==5

tempfile prev
save `prev', replace

restore

keep if measure_id==6

tempfile inc
save `inc', replace

** *********** save results **********************************************************************************************************************

use `prev',clear

levelsof(location_id), local(ids) clean
levelsof(year_id), local(years) clean

global sex_id "1 2"

foreach location_id of local ids {
		foreach year_id of local years {
			foreach sex_id of global sex_id {
					qui outsheet if location_id==`location_id' & year_id==`year_id' & sex_id==`sex_id' using "FILEPATH", comma replace
				}
			}
		}
		

		


use `inc',clear

levelsof(location_id), local(ids) clean
levelsof(year_id), local(years) clean

global sex_id "1 2"

foreach location_id of local ids {
		foreach year_id of local years {
			foreach sex_id of global sex_id {
					qui outsheet if location_id==`location_id' & year_id==`year_id' & sex_id==`sex_id' using "FILEPATH", comma replace
				}
			}
		}
		

		

run "FILEPATH"
save_results, modelable_entity_id(9969) description(TB no-HIV, `model_version_id') mark_best(yes) in_dir(ADDRESS) metrics(prevalence incidence)


** ***********************************************************************************************************************************************
** calculate drug sensitive TB no-HIV
** ***********************************************************************************************************************************************

clear all
adopath + "ADDRESS"
get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") location_set_id(22) clear
rename population mean_pop
tempfile pop_all
save `pop_all', replace
		
adopath + "ADDRESS"
get_draws, gbd_id_field(modelable_entity_id) gbd_id(9969) measure_ids(5 6) status(best) source(epi) clear
keep measure_id location_id year_id age_group_id sex_id draw_*
save "FILEPATH", replace 

merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

forvalues i = 0/999 {
			  replace draw_`i'=draw_`i'*mean_pop
			}

tempfile tb_noHIV
save `tb_noHIV', replace
merge m:1 location_id year_id age_group_id sex_id using "FILEPATH", keep(3)nogen

// rename draws
forvalues i = 0/999 {
			  replace draw_`i'=draw_`i'*tb_nohiv_mdr_prop_`i'
			}
drop tb_nohiv_mdr_prop_*


// rename draws
forvalues i = 0/999 {
			  rename draw_`i' mdrtb_`i'
			}
tempfile mdrtb
save `mdrtb', replace

************************************************************

// merge the files
			use `tb_noHIV', clear
			merge 1:1 measure_id location_id year_id age_group_id sex_id using `mdrtb', keep(3) nogen 


// calculate non-MDRTB

// loop through draws and subtract hiv_tb from hiv 
		forvalues i=0/999 {
			replace draw_`i'=draw_`i'-mdrtb_`i'
			}

		// loop through draws and calculate rate
		forvalues i=0/999 {
			replace draw_`i'=draw_`i'/mean_pop
			}
		keep measure_id location_id year_id age_group_id sex_id draw_*

		gen modelable_entity_id=10829

preserve

keep if measure_id==5

tempfile prev
save `prev', replace

restore

keep if measure_id==6

tempfile inc
save `inc', replace

** *********** save results **********************************************************************************************************************

use `prev',clear

levelsof(location_id), local(ids) clean
levelsof(year_id), local(years) clean

global sex_id "1 2"

foreach location_id of local ids {
		foreach year_id of local years {
			foreach sex_id of global sex_id {
					qui outsheet if location_id==`location_id' & year_id==`year_id' & sex_id==`sex_id' using "FILEPATH", comma replace
				}
			}
		}
		

		


use `inc',clear

levelsof(location_id), local(ids) clean
levelsof(year_id), local(years) clean

global sex_id "1 2"

foreach location_id of local ids {
		foreach year_id of local years {
			foreach sex_id of global sex_id {
					qui outsheet if location_id==`location_id' & year_id==`year_id' & sex_id==`sex_id' using "FILEPATH", comma replace
				}
			}
		}
		

		

run "FILEPATH"
save_results, modelable_entity_id(10829) description(drug_sensitive,`model_version_id') mark_best(yes) in_dir(ADDRESS) metrics(prevalence incidence)




** ***********************************************************************************************************************************************
** calculate TB no-HIV MDR
** ***********************************************************************************************************************************************

clear all
adopath + "ADDRESS"
get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") location_set_id(22) clear
rename population mean_pop
tempfile pop_all
save `pop_all', replace

use "FILEPATH", clear

merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

forvalues i = 0/999 {
			  replace draw_`i'=draw_`i'*mean_pop
			}

tempfile tb_noHIV
save `tb_noHIV', replace
merge m:1 location_id year_id age_group_id sex_id using "FILEPATH", keepusing(tb_nohiv_mdr_prop) keep(3)nogen

// rename draws
forvalues i = 0/999 {
			  replace draw_`i'=draw_`i'*tb_nohiv_mdr_prop_`i'
			}
drop tb_nohiv_mdr_prop_*


// rename draws
forvalues i = 0/999 {
			  rename draw_`i' mdrtb_`i'
			}
tempfile mdrtb
save `mdrtb', replace
*/

// get SR names

use "FILEPATH", clear

keep location_id super_region_name

tempfile sr
save `sr', replace


use `mdrtb', clear

merge m:1 location_id using `sr', keep(3)nogen

/* merge m:1 super_region_name using "FILEPATH", keep(3)nogen  */
merge m:1 super_region_name year_id using "FILEPATH", keep(3)nogen

// calculate XDR
forvalues i=0/999 {
			gen draw_`i'=mdrtb_`i'*XDR_prop
			}
			
keep measure_id location_id year_id age_group_id sex_id draw_*

** **************************************************
preserve 
keep if year<1993
forvalues i=0/999 {
	replace draw_`i'=0 
}
tempfile zero
save `zero', replace
restore
drop if year<1993
append using `zero'
** ***************************************************

gen modelable_entity_id=10831

tempfile xdr
save `xdr', replace



// calculate MDR cases

// rename draws
forvalues i = 0/999 {
			  rename draw_`i' xdr_`i'
			}

merge 1:1 measure_id location_id year_id age_group_id sex using `mdrtb', keep(3) nogen 

// loop through draws and subtract xdr from mdr
		forvalues i=0/999 {
			gen draw_`i'=mdrtb_`i'-xdr_`i'
			}
keep measure_id location_id year_id age_group_id sex_id draw_*

gen modelable_entity_id=10830


// convert MDR cases into rate

merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

// loop through draws and calculate rate
		forvalues i=0/999 {
			replace draw_`i'=draw_`i'/mean_pop
			}
drop mean_pop
preserve

keep if measure_id==5

tempfile prev
save `prev', replace

restore

keep if measure_id==6

tempfile inc
save `inc', replace
** *********** save results **********************************************************************************************************************

use `prev',clear

levelsof(location_id), local(ids) clean
levelsof(year_id), local(years) clean

global sex_id "1 2"

foreach location_id of local ids {
		foreach year_id of local years {
			foreach sex_id of global sex_id {
					qui outsheet if location_id==`location_id' & year_id==`year_id' & sex_id==`sex_id' using "/FILEPATH", comma replace
				}
			}
		}
		

		


use `inc',clear

levelsof(location_id), local(ids) clean
levelsof(year_id), local(years) clean

global sex_id "1 2"

foreach location_id of local ids {
		foreach year_id of local years {
			foreach sex_id of global sex_id {
					qui outsheet if location_id==`location_id' & year_id==`year_id' & sex_id==`sex_id' using "FILEPATH", comma replace
				}
			}
		}
		

		

run "FILEPATH"
save_results, modelable_entity_id(10830) description(mdr,`model_version_id') mark_best(yes) in_dir(ADDRESS) metrics(prevalence incidence)

** ********************************************************************************************************************************

// convert XDR cases into rates

use `xdr', clear

merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

// loop through draws and calculate rate
		forvalues i=0/999 {
			replace draw_`i'=draw_`i'/mean_pop
			}
drop mean_pop
preserve

keep if measure_id==5

tempfile prev
save `prev', replace

restore

keep if measure_id==6

tempfile inc
save `inc', replace
** *********** save results **********************************************************************************************************************

use `prev',clear

levelsof(location_id), local(ids) clean
levelsof(year_id), local(years) clean

global sex_id "1 2"

foreach location_id of local ids {
		foreach year_id of local years {
			foreach sex_id of global sex_id {
					qui outsheet if location_id==`location_id' & year_id==`year_id' & sex_id==`sex_id' using "FILEPATH", comma replace
				}
			}
		}
		

		


use `inc',clear

levelsof(location_id), local(ids) clean
levelsof(year_id), local(years) clean

global sex_id "1 2"

foreach location_id of local ids {
		foreach year_id of local years {
			foreach sex_id of global sex_id {
					qui outsheet if location_id==`location_id' & year_id==`year_id' & sex_id==`sex_id' using "FILEPATH", comma replace
				}
			}
		}
		

		

run "FILEPATH"
save_results, modelable_entity_id(10831) description(xdr,`model_version_id') mark_best(yes) in_dir(ADDRESS) metrics(prevalence incidence)
