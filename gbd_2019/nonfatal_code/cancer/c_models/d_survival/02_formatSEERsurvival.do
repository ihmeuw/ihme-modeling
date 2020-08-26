/////////////////////////////////////////////////////////
// Name of script: formatSEERsurvival.do
// Description: Imports SEER survival data downloaded from SEER*Stat; 
// 		cleans and preps imported data for use in survival estimation 
// Arguments: Requires downloaded SEER data in appropriate format.
// Outputs: Cancer-specific *.txt datasets for next prep step
// Author(s): NAME
// Last updated: 2018-10-04
	
	
/////////////////////////////
// Format SEER survival datasets

set more off

global SEERdata "FILEPATH"

local cancers "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_meso neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"


foreach cancer of local cancers {
	import delimited "${SEERdata}\\`cancer'_SEERsurv10yrs.txt"
	
	gen acause = "`cancer'"
	
	drop if sex==0
	gen sex_id = sex
	
	gen year_range = "yrs_2000_2009" if year_dx_aggregate == 16
	replace year_range = "yrs_2001_2010" if year_dx_aggregate == 17
	replace year_range = "yrs_2005_2014" if year_dx_aggregate == 18
	replace year_range = "yrs_2000_2014" if year_dx_aggregate == 19
	
	gen age_group_id = age_gbd + 5 if age_gbd < 16
	replace age_group_id = 1 if age_gbd == 0
	replace age_group_id = 30 if age_gbd == 16
	replace age_group_id = 21 if age_gbd == 17
	replace age_group_id = 160 if age_gbd == 18
	
	gen surv_year = summaryinterval + 1
	
	gen SEER_surv = relative / 100
	
	export delimited using "${SEERdata}\\`cancer'_SEERsurv10yrs_formatted.txt", replace
	
	clear
	
}	
	