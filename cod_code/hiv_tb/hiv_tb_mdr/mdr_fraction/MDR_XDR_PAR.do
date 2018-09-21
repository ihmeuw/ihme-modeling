** Description: calculate population attributable risks for MDR-TB and XDR-TB

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
				
			

** *********************************************************************************************************
** calculate MDR PAR for HIV-TB
** *********************************************************************************************************

// bring in MDR-HIVTB proportion draws

use "FILEPATH", clear

gen acause="tb_drug"

// merge on RR draws
merge m:1 acause using "FILEPATH", keep(3)nogen

// calculate PAR

forvalues x = 0/999 {
		gen PAR_based_on_mean_rr_`x' = (hiv_mdr_prop_`x'*rr_`x')/((hiv_mdr_prop_`x'*rr_`x')+(1-hiv_mdr_prop_`x'))
				}

/* gen PAR_based_on_mean_rr=(hiv_mdr_prop*RR_mean)/((hiv_mdr_prop*RR_mean)+(1-hiv_mdr_prop))  */


keep location_id year_id age_group_id sex_id PAR_based_on_mean_rr_*

save "FILEPATH", replace

** *********************************************************************************************************
** calculate MDR PAR for TB
** *********************************************************************************************************

// bring in HIV-negative MDR TB proportion draws
use "FILEPATH", clear

gen acause="tb_drug"

// merge on RR draws
merge m:1 acause using "FILEPATH", keep(3)nogen

// calculate PAR

forvalues x = 0/999 {
		gen PAR_based_on_mean_rr_`x' = (tb_nohiv_mdr_prop_`x'*rr_`x')/((tb_nohiv_mdr_prop_`x'*rr_`x')+(1-tb_nohiv_mdr_prop_`x'))
				}
	

/* gen PAR_based_on_mean_rr=(tb_nohiv_mdr_prop*RR_mean)/((tb_nohiv_mdr_prop*RR_mean)+(1-tb_nohiv_mdr_prop))  */

keep location_id year_id age_group_id sex_id PAR_based_on_mean_rr_*

save "FILEPATH", replace

** ********************************************************************************************************
** calculate PAR for XDR TB

// bring in XDR proportions

use "FILEPATH", clear

gen acause="tb_xdr"

// merge on RR draws
merge m:1 acause using "FILEPATH", keep(3)nogen

// calculate PAR

forvalues x = 0/999 {
		gen PAR_based_on_mean_rr_`x' = (XDR_prop*rr_`x')/((XDR_prop*rr_`x')+(1-XDR_prop))
				}
drop rr_*	
drop XDR_prop acause

/* gen PAR_based_on_mean_rr=(tb_nohiv_mdr_prop*RR_mean)/((tb_nohiv_mdr_prop*RR_mean)+(1-tb_nohiv_mdr_prop))  */


save "FILEPATH", replace

