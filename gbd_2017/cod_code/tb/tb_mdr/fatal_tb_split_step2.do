
** Description: Calculating MDR-TB (without XDR) and XDR-TB 


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
					global prefix "FILEPATH"
					set odbcmgr unixodbc
				}
				else if c(os) == "FILEPATH" {
					global prefix "J:"
				}
			
			// Close any open log file
				cap log close

** *************************************************************************************************************************************************
// locals
local acause tb
local custom_version 484832_484829

// get SR names

use "FILEPATH", clear

keep location_id super_region_name

tempfile sr
save `sr', replace


insheet using "FILEPATH", comma names clear
duplicates drop location_id year_id age_group_id sex_id, force
tempfile mdr_cyas
save `mdr_cyas', replace

merge m:1 location_id using `sr', keep(3)nogen


// apply fractions of XDR-TB deaths among all MDR-TB deaths

merge m:1 super_region_name year_id using "FILEPATH", keep(3)nogen

// rename draws
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

gen cause_id=947
keep location_id year_id age_group_id sex_id cause_id draw_*
tempfile xdr
save `xdr', replace

levelsof(location_id), local(ids) clean

foreach location_id of local ids {	
		qui outsheet if location_id==`location_id' using "FILEPATH", comma replace
				}

// calculate MDR-TB without XDR

// rename draws
use `xdr', clear
forvalues i = 0/999 {
			  rename draw_`i' xdr_`i'
			}

merge 1:1 location_id year_id age_group_id sex_id using `mdr_cyas', keep(3) nogen 

// loop through draws and subtract xdr from mdr
		forvalues i=0/999 {
			replace draw_`i'=draw_`i'-xdr_`i'
			}
replace cause_id=946	

// here
keep location_id year_id age_group_id sex_id cause_id draw_*
levelsof(location_id), local(ids) clean

foreach location_id of local ids {	
		qui outsheet if location_id==`location_id' using "FILEPATH", comma replace
				}


** *********************************************************************************************************************************************************
** Step (6): upload results
** *********************************************************************************************************************************************************

// save results for xdr and mdr

run "FILEPATH"
save_results_cod, cause_id(947) description(xdr custom `custom_version') input_file_pattern({location_id}.csv) input_dir("FILEPATH") clear

// save results for tb_other

run "FILEPATH"
save_results_cod, cause_id(946) description(mdr custom `custom_version') input_file_pattern({location_id}.csv) input_dir("FILEPATH") clear

