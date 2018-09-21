** Description: Computing MDR-TB, XDR_TB, and drug-susceptible TB deaths among HIV-positive people


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

				

** ***********************************************************************************************************************************************************
// locals
local acause hiv_tb
local custom_version v2.3
local tb_model 362831_362834
local hiv_model 362765_362768

// Make folders on cluster
capture mkdir "ADDRESS"
capture mkdir "ADDRESS"
capture mkdir "ADDRESS"
capture mkdir "ADDRESS"
capture mkdir "ADDRESS"
capture mkdir "ADDRESS"
capture mkdir "ADDRESS"

// define filepaths
capture mkdir "ADDRESS"
local outdir "ADDRESS"
	
************************MDR HIV-TB*****************************************************************************************************************************

// rename hivtb death draws
insheet using "FILEPATH", comma names clear
duplicates drop location_id year_id age_group_id sex_id, force
tempfile hivtb
save `hivtb', replace

// merge on MDR-HIVTB fractions
merge 1:1 location_id year_id age_group_id sex_id using "FILEPATH", keep(3)nogen

// calculate mdr hivtb deaths
forvalues i = 0/999 {
			  replace draw_`i'=draw_`i'*PAR_based_on_mean_rr_`i'
			}
drop PAR_based_on_mean_rr_*


// rename draws
forvalues i = 0/999 {
			  rename draw_`i' mdrtb_`i'
			}
tempfile mdrtb
save `mdrtb', replace

************************Drug-susceptible HIVTB******************************************************************************************************************

// merge the files
			use `hivtb', clear
			merge 1:1 location_id year_id age_group_id sex_id using `mdrtb', keep(3) nogen 


// calculate non-MDRTB

// loop through draws and subtract mdr_hivtb from hivtb 
		forvalues i=0/999 {
			replace draw_`i'=draw_`i'-mdrtb_`i'
			}

drop mdr*			
replace cause_id=948		
outsheet using "FILEPATH", comma names replace 

**************************XDR-HIVTB*****************************************************************************************************************************

// XDR
// get SR names

use "FILEPATH", clear

keep location_id super_region_name

tempfile sr
save `sr', replace

use `mdrtb', clear
duplicates drop location_id year_id age_group_id sex_id, force

merge m:1 location_id using `sr', keep(3)nogen

// merge on XDR-HIVTB fractions

merge m:1 super_region_name year_id using "FILEPATH", keep(3)nogen

// rename draws
forvalues i = 0/999 {
			  qui gen draw_`i'=mdrtb_`i'*XDR_PAR_based_on_mean_rr_`i'
			}
drop XDR_PAR_based_on_mean_rr_* mdrtb_*



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
** **************************************************

replace cause_id=950

tempfile xdr
save `xdr', replace

outsheet using "FILEPATH", comma names replace 


***************************** MDR HIVTB without XDR *************************************************************************************************************

// rename draws
forvalues i = 0/999 {
			  rename draw_`i' xdr_`i'
			}

merge 1:1 location_id year_id age_group_id sex using `mdrtb', keep(3) nogen 

// loop through draws and subtract xdr from mdr
		forvalues i=0/999 {
			qui gen draw_`i'=mdrtb_`i'-xdr_`i'
			}
drop mdr* xdr*
replace cause_id=949		
outsheet using "FILEPATH", comma names replace


** **************************************************************************************************************************************************************
** upload results
** **************************************************************************************************************************************************************

// save results 

run save_results.do
save_results, cause_id(950) description(hivtb_xdr custom `custom_version') mark_best(yes) in_dir("FILEPATH") model_version_type_id(6)

run save_results.do
save_results, cause_id(949) description(hivtb_mdr custom `custom_version') mark_best(yes) in_dir("FILEPATH") model_version_type_id(6)
	
run save_results.do
save_results, cause_id(948) description(hivtb_drug_sensitive custom `custom_version') mark_best(yes) in_dir("FILEPATH") model_version_type_id(6)

