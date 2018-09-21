
** Description: MDR-TB without XDR and XDR-TB calculations


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
				
			// local

				

****************************************************************************************************************************************************
// locals
local acause tb_drug
local custom_version 362831_362834

// Make folders on cluster
capture mkdir "ADDRESS"
capture mkdir "ADDRESS"
capture mkdir "ADDRESS"
capture mkdir "ADDRESS"
capture mkdir "ADDRESS"

// get SR names

use "FILEPATH", clear

keep location_id super_region_name

tempfile sr
save `sr', replace

***************************** Compute XDR-TB mortality *******************************************************************************************************************

// bring in MDR-TB death draws

insheet using FILEPATH, comma names clear
duplicates drop location_id year_id age_group_id sex_id, force
tempfile mdr_cyas
save `mdr_cyas', replace

merge m:1 location_id using `sr', keep(3)nogen

// merge on XDR-TB fraction draws

merge m:1 super_region_name year_id using "FILEPATH", keep(3)nogen

// compute XDR mortality
forvalues i = 0/999 {
			  replace draw_`i'=draw_`i'*PAR_based_on_mean_rr_`i'
			}
drop PAR_based_on_mean_rr_*


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

replace cause_id=947

tempfile xdr
save `xdr', replace

outsheet using FILEPATH, comma names replace 


***************************** Compute MDR-TB (without XDR) mortality *****************************************************************************************************


// rename XDR death draws
forvalues i = 0/999 {
			  rename draw_`i' xdr_`i'
			}

// merge on MDR death draws
merge 1:1 location_id year_id age_group_id sex using `mdr_cyas', keep(3) nogen 

// loop through draws and subtract xdr from mdr
		forvalues i=0/999 {
			replace draw_`i'=draw_`i'-xdr_`i'
			}
replace cause_id=946		
outsheet using FILEPATH, comma names replace 


** *********************************************************************************************************************************************************
** Upload results
** *********************************************************************************************************************************************************

// save results for XDR-TB 

run FILEPATH
save_results, cause_id(947) description(xdr custom `custom_version') mark_best(yes) in_dir(ADDRESS)


// save results for MDR-TB without XDR

run FILEPATH
save_results, cause_id(946) description(mdr custom `custom_version') mark_best(yes) in_dir(ADDRESS) 
