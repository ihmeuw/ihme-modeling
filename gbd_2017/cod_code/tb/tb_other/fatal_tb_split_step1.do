** Description: MDR-TB and drug-susceptible-TB calculations


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
				else if c(os) == "Windows" {
					global prefix "FILEPATH"
				}
			
			// Close any open log file
				cap log close

** *************************************************************************************************************************************************
// locals
local acause tb
local custom_version 484832_484829
local lri_version 484013_483782_483785_484016
				
adopath+ "FILEPATH"

// Adding deaths diagnosed with both pneumonia and TB among children <15 years
use FILEPATH, clear
duplicates drop location_id year_id age_group_id sex_id, force
append using "FILEPATH"
fastcollapse draw_*, type(sum) by(location_id year_id age_group_id sex_id) 
tempfile tb
save `tb', replace
save FILEPATH, replace

merge 1:1 location_id year_id age_group_id sex_id using "FILEPATH", keep(3)nogen

// rename draws
forvalues i = 0/999 {
			  replace draw_`i'=draw_`i'*PAR_based_on_mean_rr_`i'
			}
drop PAR_based_on_mean_rr_*

outsheet using "FILEPATH", comma names replace

// rename draws
forvalues i = 0/999 {
			  rename draw_`i' mdrtb_`i'
			}
tempfile mdrtb
save `mdrtb', replace

************************************************************

// merge the files
			use `tb', clear
			merge 1:1 location_id year_id age_group_id sex_id using `mdrtb', keep(3) nogen 


// calculate drug-susceptible TB

// loop through draws and subtract mdr_tb from tb
		forvalues i=0/999 {
			replace draw_`i'=draw_`i'-mdrtb_`i'
			replace draw_`i'=0 if draw_`i'<0
			}
gen cause_id=934		

keep location_id year_id age_group_id sex_id cause_id draw_*

levelsof(location_id), local(ids) clean

foreach location_id of local ids {	
		qui outsheet if location_id==`location_id' using "FILEPATH", comma replace
				}

** *********************************************************************************************************************************************************
** upload results
** *********************************************************************************************************************************************************
// save results for drug-susceptible TB

run "FILEPATH"
save_results_cod, cause_id(934) description(tb_drug_susp custom `custom_version') input_file_pattern({location_id}.csv) input_dir("FILEPATH") clear



** *********************************************************************************************************************************************************
** upload TB parent 
** *********************************************************************************************************************************************************	

use `tb', clear

gen cause_id=297

levelsof(location_id), local(ids) clean

foreach location_id of local ids {	
		qui outsheet if location_id==`location_id' using "FILEPATH", comma replace
				}

// save results for TB parent

run "FILEPATH"
save_results_cod, cause_id(297) description(TB parent `custom_version', child TB adjusted) input_file_pattern({location_id}.csv) input_dir("FILEPATH") clear
