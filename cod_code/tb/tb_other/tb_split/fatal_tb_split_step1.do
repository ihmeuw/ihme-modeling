** Description: MDR-TB and non-MDR-TB calculations


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

				

** *************************************************************************************************************************************************
// locals
local acause tb_drug
local custom_version 362831_362834


// Make folders on cluster
capture mkdir "ADDRESS"
capture mkdir "ADDRESS"


// define filepaths
	cap mkdir "ADDRESS"
	cap mkdir "ADDRESS"
	
	local outdir "ADDRESS"
	local indir "ADDRESS"
	local tempdir "ADDRESS"
	

// rename tb death draws
use FILEPATH, clear
duplicates drop location_id year_id age_group_id sex_id, force
tempfile tb
save `tb', replace

// merge on population attributable fractions
merge 1:1 location_id year_id age_group_id sex_id using "FILEPATH", keep(3)nogen

// compute MDR death draws
forvalues i = 0/999 {
			  replace draw_`i'=draw_`i'*PAR_based_on_mean_rr_`i'
			}
drop PAR_based_on_mean_rr_*

outsheet using FILEPATH, comma names replace

// rename MDR-TB deaths draws
forvalues i = 0/999 {
			  rename draw_`i' mdrtb_`i'
			}
tempfile mdrtb
save `mdrtb', replace

************************************************************

// merge the files
			use `tb', clear
			merge 1:1 location_id year_id age_group_id sex_id using `mdrtb', keep(3) nogen 


// calculate drug_susceptible TB

// loop through draws and subtract hiv_tb from hiv 
		forvalues i=0/999 {
			replace draw_`i'=draw_`i'-mdrtb_`i'
			}
replace cause_id=934		

keep location_id year_id age_group_id sex_id cause_id draw_*

outsheet using FILEPATH, comma names replace 


** *********************************************************************************************************************************************************
** upload results
** *********************************************************************************************************************************************************

// save results for drug-susceptible tb

run FILEPATH
save_results, cause_id(934) description(tb_drug_sensitive custom `custom_version') mark_best(yes) in_dir(ADDRESS) 


	