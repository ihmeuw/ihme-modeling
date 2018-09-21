

** Description: split HIV-TB into MDR, XDR, and drug-sensitive HIVTB
				
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
				local acause hiv_tb
				local model_version_id dismod_189863
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



** ***********************************************************************************************************************************************
** Split HIV-TB into MDR, XDR, and drug-sensitive HIVTB
** ***********************************************************************************************************************************************
// get population
clear all
adopath + "ADDRESS"
get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") location_set_id(22) clear
rename population mean_pop
tempfile pop_all
save `pop_all', replace

// pull HIVTB epi draws
adopath + "ADDRESS"
get_draws, gbd_id_field(modelable_entity_id) gbd_id(1176) measure_ids(5 6) status(best) source(epi) clear
keep measure_id location_id year_id age_group_id sex_id draw_*

merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

// convert rates to cases
forvalues i = 0/999 {
			  replace draw_`i'=draw_`i'*mean_pop
			}

tempfile hivtb
save `hivtb', replace
merge m:1 location_id year_id age_group_id sex_id using "FILEPATH", keep(3)nogen

// apply mdr proportions
forvalues i = 0/999 {
			  replace draw_`i'=draw_`i'*hiv_mdr_prop_`i'
			}
drop hiv_mdr_prop_*


// rename draws
forvalues i = 0/999 {
			  rename draw_`i' mdrtb_`i'
			}
tempfile mdrtb
save `mdrtb', replace

************************************************************

// merge the files
			use `hivtb', clear
			merge 1:1 measure_id location_id year_id age_group_id sex_id using `mdrtb', keep(3) nogen 

** **********************************************************************************************************************************************
** calculate drug-sensitive HIVTB
** **********************************************************************************************************************************************

// loop through draws and subtract mdr_hivtb from hivtb 
		forvalues i=0/999 {
			replace draw_`i'=draw_`i'-mdrtb_`i'
			replace draw_`i'=0 if draw_`i'<0
			}

		// loop through draws and calculate rate
		forvalues i=0/999 {
			replace draw_`i'=draw_`i'/mean_pop
			}
		keep measure_id location_id year_id age_group_id sex_id draw_*

		gen modelable_entity_id=10832

preserve

keep if measure_id==5

tempfile prev
save `prev', replace

restore

keep if measure_id==6

tempfile inc
save `inc', replace

** *********** save results for drug-sensitive HIV-TB**********************************************************************************************************************

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
save_results, modelable_entity_id(10832) description(drug_sensitive hivtb, `model_version_id') mark_best(yes) in_dir(ADDRESS) metrics(prevalence incidence)


** ***********************************************************************************************************************************************
** calculate XDR-HIVTB 
** ***********************************************************************************************************************************************


// get SR names

use "FILEPATH", clear

keep location_id super_region_name

tempfile sr
save `sr', replace


use `mdrtb', clear

merge m:1 location_id using `sr', keep(3)nogen

/* merge m:1 super_region_name using "FILEPATH", keep(3)nogen */
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

gen modelable_entity_id=10834

tempfile xdr
save `xdr', replace


** ***********************************************************************************************************************************************
** calculate MDR-HIVTB (without XDR) 
** ***********************************************************************************************************************************************

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

gen modelable_entity_id=10833


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
** *********** save results for MDR-HIVTB**********************************************************************************************************************

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
save_results, modelable_entity_id(10833) description(mdr hivtb, `model_version_id') mark_best(yes) in_dir(ADDRESS) metrics(prevalence incidence)

** **********************************************************************************************************************************************************

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

** *********** save results for XDR-HIVTB**********************************************************************************************************************

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
save_results, modelable_entity_id(10834) description(xdr hivtb, `model_version_id') mark_best(yes) in_dir(ADDRESS) metrics(prevalence incidence)



//After everything is completed, qsub to launch the HIV proportion splitting code

local file_location "ADDRESS"
!qsub -N "start_hiv_split" -P "proj_tb" -o "ADDRESS" -e "ADDRESS" "FILEPATH" "FILEPATH"
