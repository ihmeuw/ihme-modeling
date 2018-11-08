** Description: split TB no-HIV into drug-susceptible, MDR-TB and XDR-TB

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
				local model_version_id dismod_332315
				local ret_model_version_id dismod_332315_ret
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
** ***********************************************************************************************************************************************
** calculate drug susceptible TB no-HIV
** ***********************************************************************************************************************************************

clear all
adopath + "/ADDRESS/"
get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") location_set_version_id(319) clear
rename population mean_pop
tempfile pop_all
save `pop_all', replace

/*		
adopath + "/ADDRESS/"
get_draws, gbd_id_type(modelable_entity_id) gbd_id(9969) measure_id(5 6) status(best) num_workers(15) source(epi) clear
keep measure_id location_id year_id age_group_id sex_id draw_*
save "/FILEPATH/", replace 
*/

use "/FILEPATH/", clear 
merge m:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(mean_pop) keep(3)nogen

forvalues i = 0/999 {
			  replace draw_`i'=draw_`i'*mean_pop
			}

tempfile tb_noHIV
save `tb_noHIV', replace
merge m:1 location_id year_id age_group_id sex_id using "/FILEPATH/", keep(3)nogen


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


**************************************** Compute drug-susceptible TB ***************************************************************************

// merge the files
			use `tb_noHIV', clear
			merge 1:1 measure_id location_id year_id age_group_id sex_id using `mdrtb', keep(3) nogen 


// calculate non-MDRTB

// loop through draws and subtract mdr_tb from tb 
		forvalues i=0/999 {
			replace draw_`i'=draw_`i'-mdrtb_`i'
			}

		// loop through draws and calculate rate
		forvalues i=0/999 {
			replace draw_`i'=draw_`i'/mean_pop
			replace draw_`i'=0 if draw_`i'<0
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

foreach location_id of local ids {
		
					qui outsheet if location_id==`location_id' using "/FILEPATH/", comma replace
				
			}
		


use `inc',clear

levelsof(location_id), local(ids) clean

foreach location_id of local ids {
					qui outsheet if location_id==`location_id' using "/FILEPATH/", comma replace
			
			}
		
*/

** ***********************************************************************************************************************************************
** calculate TB no-HIV MDR
** ***********************************************************************************************************************************************

// get SR names

use "/FILEPATH/", clear

keep location_id super_region_name

tempfile sr
save `sr', replace


use `mdrtb', clear

merge m:1 location_id using `sr', keep(3)nogen

/* merge m:1 super_region_name using "/FILEPATH/", keep(3)nogen  */
merge m:1 super_region_name year_id using "/FILEPATH/", keep(3)nogen

// calculate XDR
forvalues i=0/999 {
			gen draw_`i'=mdrtb_`i'*XDR_prop
			replace draw_`i'=0 if draw_`i'<0
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
			replace draw_`i'=0 if draw_`i'<0
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

tempfile mdr_prev
save `mdr_prev', replace

restore


keep if measure_id==6
forvalues i=0/999 {
			rename draw_`i' no_ret_draw_`i'
			}	

tempfile mdr_inc
save `mdr_inc', replace

** *********** save results **********************************************************************************************************************

use `mdr_prev',clear

levelsof(location_id), local(ids) clean

foreach location_id of local ids {
		
					qui outsheet if location_id==`location_id' using"/FILEPATH/", comma replace
				
			}
		



use "/FILEPATH/", clear

replace modelable_entity_id=10830

levelsof(location_id), local(ids) clean

foreach location_id of local ids {
					qui outsheet if location_id==`location_id' using "/FILEPATH/", comma replace
			
			}


// compute the MDR cases to be added back

use "/FILEPATH/", clear
// rename
forvalues i=0/999 {
			rename draw_`i' ret_draw_`i'
			}
merge 1:1 location_id year_id age_group_id sex_id using `mdr_inc', keep(3) nogen
	forvalues i=0/999 {
			gen draw_`i'= ret_draw_`i'-no_ret_draw_`i'
			}	
save "/FILEPATH/", replace



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

tempfile xdr_prev
save `xdr_prev', replace

restore


keep if measure_id==6
forvalues i=0/999 {
			rename draw_`i' no_ret_draw_`i'
			}	
tempfile xdr_inc
save `xdr_inc', replace

** *********** save results **********************************************************************************************************************

use `xdr_prev',clear

levelsof(location_id), local(ids) clean

foreach location_id of local ids {
		
					qui outsheet if location_id==`location_id' using "/FILEPATH/", comma replace
				
			}
		



use "/FILEPATH/", clear

replace modelable_entity_id=10831

levelsof(location_id), local(ids) clean

foreach location_id of local ids {
					qui outsheet if location_id==`location_id' using "/FILEPATH/", comma replace
			
			}
		
		
// compute the XDR cases to be added back

use "/FILEPATH/", clear

// rename
forvalues i=0/999 {
			rename draw_`i' ret_draw_`i'
			}
merge 1:1 location_id year_id age_group_id sex_id using `xdr_inc', keep(3) nogen
	forvalues i=0/999 {
			gen draw_`i'= ret_draw_`i'-no_ret_draw_`i'
			}	
save "/FILEPATH/", replace

******************************************************upload****************************************************************************************

// drug susceptible

run "/FILEPATH/"

save_results_epi, input_dir("/ADDRESS/") input_file_pattern({location_id}_{measure_id}.csv) modelable_entity_id(10829) mark_best("True") description("drug susceptible, `model_version_id'") measure_id(5 6) db_env("prod") clear 

// mdr
run "/FILEPATH/"

save_results_epi, input_dir("/ADDRESS/") input_file_pattern({location_id}_{measure_id}.csv) modelable_entity_id(10830) mark_best("True") description("mdr, `model_version_id'") measure_id(5 6) db_env("prod") clear 

// xdr
run "/FILEPATH/"

save_results_epi, input_dir("/ADDRESS/") input_file_pattern({location_id}_{measure_id}.csv) modelable_entity_id(10831) mark_best("True") description("xdr, `model_version_id'") measure_id(5 6) db_env("prod") clear 
