
/////////////////////////////////////////////////////////
// Name of script: formatSEERMIR.do
// Description: Imports mortality, incidence, and survival data that have been
//		downloaded from SEER; merges these together and performs operations
//		to prepare them for further analyses.
// Arguments: 
// Outputs: Cancer-specific *.txt datasets for use in survival modeling process
// Author(s): NAME
// Last updated: 2018-10-04


/////////////////////////////////////////////////////////
// Generating datasets for SEER-based MIRs, 5-year aggregates
/////////////////////////////////////////////////////////

// Data to be used was downloaded via SEER*Stat:
// 	Rate Session: Mortality - All COD, Aggregated With State, Total U.S. 
// 		(1969-2015) <Katrina/Rita Population Adjustment>
// 	Rate Session: Incidence - SEER 9 Regs Research Data, Nov 2016 Sub 
//		(1973-2014) <Katrina/Rita Population Adjustment>
// 	Survival Session: Incidence - SEER 9 Regs Research Data, Nov 2016 Sub 
//		(1973-2014) <Katrina/Rita Population Adjustment>

////// Workflow of the below code:
// 1) import/format/export SEER incidence data
// 2) import/format/export SEER mortality data
// 3) import/format/export SEER survival data
// 4) import/format/export GBD MIR for mesothelioma (unavailable in SEER)
// 5) combine/format/export above into cancer-specific SEER MIR datasets


// set parameters and paths

set more off

// local for all cancers
local cancers_all "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_meso neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"

// set cancers to iterate across (doesn't include neo_meso, as not reported in SEER)
local cancers "neo_bladder"
// neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine" 

// set working directory containing SEER input data
global SEERdata "FILEPATH"

// set working directory for compiled MIR data
global compiledMIRs "FILEPATH"

// set working directory for output data
global SEERdataOutput "FILEPATH"


/////////////////////////////
// 1) Format incidence datasets (5-year aggregates)

foreach cancer of local cancers_all {
	import delimited "${SEERdata}\\`cancer'_SEER_inc_5years.txt"

	rename cruderate inc_crude_rate
	rename count inc_count
	rename population inc_population	
	
	gen acause = "`cancer'"
	
	drop if sex==0
	
	gen location_id = 102	
	
	gen year5 = ""
	replace year5 = "years_1980_1984" if yearofdiagnosis_5yearaggregates == 0
	replace year5 = "years_1985_1989" if yearofdiagnosis_5yearaggregates == 1
	replace year5 = "years_1990_1994" if yearofdiagnosis_5yearaggregates == 2
	replace year5 = "years_1995_1999" if yearofdiagnosis_5yearaggregates == 3
	replace year5 = "years_2000_2004" if yearofdiagnosis_5yearaggregates == 4
	replace year5 = "years_2005_2009" if yearofdiagnosis_5yearaggregates == 5
	replace year5 = "years_2010_2014" if yearofdiagnosis_5yearaggregates == 6

	gen age_group_id = .
	replace age_group_id = 1 if agegbdcancer == 0
	replace age_group_id = 6 if agegbdcancer == 1
	replace age_group_id = 7 if agegbdcancer == 2
	replace age_group_id = 8 if agegbdcancer == 3
	replace age_group_id = 9 if agegbdcancer == 4
	replace age_group_id = 10 if agegbdcancer == 5
	replace age_group_id = 11 if agegbdcancer == 6
	replace age_group_id = 12 if agegbdcancer == 7
	replace age_group_id = 13 if agegbdcancer == 8
	replace age_group_id = 14 if agegbdcancer == 9
	replace age_group_id = 15 if agegbdcancer == 10
	replace age_group_id = 16 if agegbdcancer == 11
	replace age_group_id = 17 if agegbdcancer == 12
	replace age_group_id = 18 if agegbdcancer == 13
	replace age_group_id = 19 if agegbdcancer == 14
	replace age_group_id = 20 if agegbdcancer == 15
	replace age_group_id = 21 if agegbdcancer == 16

	export delimited using "${SEERdata}\\`cancer'_SEERinc5_formatted.txt", replace
	
	save "${SEERdata}\\`cancer'_SEERinc5_formatted.dta", replace

	clear
}	

/////////////////////////////
// 2) Format mortality datasets (5 year aggregates)

foreach cancer of local cancers_all {
	import delimited "${SEERdata}\\`cancer'_SEER_mort_5years.txt"

	gen acause = "`cancer'"
		
	rename cruderate mort_crude_rate
	rename count mort_count
	rename population mort_population	
	
	drop if sex==0
	
	gen location_id = 102	
	
	gen year5 = ""
	replace year5 = "years_1980_1984" if yearofdeath5yeargroups == 0
	replace year5 = "years_1985_1989" if yearofdeath5yeargroups == 1
	replace year5 = "years_1990_1994" if yearofdeath5yeargroups == 2
	replace year5 = "years_1995_1999" if yearofdeath5yeargroups == 3
	replace year5 = "years_2000_2004" if yearofdeath5yeargroups == 4
	replace year5 = "years_2005_2009" if yearofdeath5yeargroups == 5
	replace year5 = "years_2010_2014" if yearofdeath5yeargroups == 6
	
	gen age_group_id = .
	replace age_group_id = 1 if agegbdcancer == 0
	replace age_group_id = 6 if agegbdcancer == 1
	replace age_group_id = 7 if agegbdcancer == 2
	replace age_group_id = 8 if agegbdcancer == 3
	replace age_group_id = 9 if agegbdcancer == 4
	replace age_group_id = 10 if agegbdcancer == 5
	replace age_group_id = 11 if agegbdcancer == 6
	replace age_group_id = 12 if agegbdcancer == 7
	replace age_group_id = 13 if agegbdcancer == 8
	replace age_group_id = 14 if agegbdcancer == 9
	replace age_group_id = 15 if agegbdcancer == 10
	replace age_group_id = 16 if agegbdcancer == 11
	replace age_group_id = 17 if agegbdcancer == 12
	replace age_group_id = 18 if agegbdcancer == 13
	replace age_group_id = 19 if agegbdcancer == 14
	replace age_group_id = 20 if agegbdcancer == 15
	replace age_group_id = 21 if agegbdcancer == 16

	export delimited using "${SEERdata}\\`cancer'_SEERmort5_formatted.txt", replace
	
	save "${SEERdata}\\`cancer'_SEERmort5_formatted.dta", replace

	clear
}	
	
/////////////////////////////
// 3) Format survival datasets

foreach cancer of local cancers_all {
	import delimited "${SEERdata}\\`cancer'_SEER_surv_5years.txt"
	
	gen acause = "`cancer'"
	
	drop if sex==0
	
	gen location_id = 102	
	
	gen year5 = ""
	replace year5 = "years_1980_1984" if yearofdiagnosis_5yearaggregates == 0
	replace year5 = "years_1985_1989" if yearofdiagnosis_5yearaggregates == 1
	replace year5 = "years_1990_1994" if yearofdiagnosis_5yearaggregates == 2
	replace year5 = "years_1995_1999" if yearofdiagnosis_5yearaggregates == 3
	replace year5 = "years_2000_2004" if yearofdiagnosis_5yearaggregates == 4
	replace year5 = "years_2005_2009" if yearofdiagnosis_5yearaggregates == 5
	replace year5 = "years_2010_2014" if yearofdiagnosis_5yearaggregates == 6
	
	gen age_group_id = .
	replace age_group_id = 1 if agegbdcancer == 0
	replace age_group_id = 6 if agegbdcancer == 1
	replace age_group_id = 7 if agegbdcancer == 2
	replace age_group_id = 8 if agegbdcancer == 3
	replace age_group_id = 9 if agegbdcancer == 4
	replace age_group_id = 10 if agegbdcancer == 5
	replace age_group_id = 11 if agegbdcancer == 6
	replace age_group_id = 12 if agegbdcancer == 7
	replace age_group_id = 13 if agegbdcancer == 8
	replace age_group_id = 14 if agegbdcancer == 9
	replace age_group_id = 15 if agegbdcancer == 10
	replace age_group_id = 16 if agegbdcancer == 11
	replace age_group_id = 17 if agegbdcancer == 12
	replace age_group_id = 18 if agegbdcancer == 13
	replace age_group_id = 19 if agegbdcancer == 14
	replace age_group_id = 20 if agegbdcancer == 15
	replace age_group_id = 21 if agegbdcancer == 16

	export delimited using "${SEERdata}\\`cancer'_SEERsurv5_formatted.txt", replace
	
	save "${SEERdata}\\`cancer'_SEERsurv5_formatted.dta", replace

	clear
	
}	
		
/////////////////////////////		
//// 4) use GBD estimates for neo_meso (since SEER doesn't report)

import delimited "${compiledMIRs}\\current_compiled_mir_estimates.csv"

keep if location_id == 102
keep if acause == "neo_meso"

gen year5 = ""
replace year5 = "years_1980_1984" if year >= 1980 & year < 1985
replace year5 = "years_1985_1989" if year >= 1985 & year < 1990
replace year5 = "years_1990_1994" if year >= 1990 & year < 1995
replace year5 = "years_1995_1999" if year >= 1995 & year < 2000
replace year5 = "years_2000_2004" if year >= 2000 & year < 2005
replace year5 = "years_2005_2009" if year >= 2005 & year < 2010
replace year5 = "years_2010_2014" if year >= 2010 & year < 2015

sort year5 sex age_group_id
by year5 sex age_group_id: egen mi_ratio = mean(mi_ratio_old)
rename mi_ratio mi_ratio_old
rename mi_ratio_avg mi_ratio

drop if year > 1980 & year < 1985
drop if year > 1985 & year < 1990
drop if year > 1990 & year < 1995
drop if year > 1995 & year < 2000
drop if year > 2000 & year < 2005
drop if year > 2005 & year < 2010
drop if year > 2010 

save "${SEERdata}\\neo_meso_GBDmir5_formatted.dta", replace


	
//////////////////////////////
// 5) Merging inc/mort datasets, then merging survival dataset, exporting

// first merge inc/mort/surv data for all cancers but meso
foreach cancer of local cancers_all {	
	use "${SEERdata}\\`cancer'_SEERmort5_formatted.dta", clear
	
		merge 1:1 acause age_group_id year5 sex using "${SEERdata}\\`cancer'_SEERinc5_formatted.dta", generate("`cancer'_Minc")
		gen mi_ratio = mort_crude_rate / inc_crude_rate
		gen invmir = 1 - mi_ratio
	
		merge 1:m acause age_group_id year5 sex using "${SEERdata}\\`cancer'_SEERsurv5_formatted.dta" , generate("`cancer'_Msurv")
		gen rel = relative / 100
		gen exp = expected / 100
		gen obs = expected / 100	
		gen deathrate = 1 - rel			
		gen interval = summaryinterval + 1		
		gen logit_surv = ln(rel / (1 - rel))
		gen source_MIR = "SEER"
		
		export delimited using "${SEERdataOutput}\\`cancer'_SEER_5yr_MIRsurv.txt", replace
}

// next merge inc/mort/surv for neo_meso
use "${SEERdata}\\neo_meso_GBDmir5_formatted.dta", clear
merge 1:m acause age_group_id year5 sex using "${SEERdata}\\neo_meso_SEERsurv5_formatted.dta" , generate("neo_meso_Msurv")
		
gen invmir = 1 - mi_ratio
gen rel = relative / 100
gen exp = expected / 100
gen obs = expected / 100	
gen deathrate = 1 - rel			
gen interval = summaryinterval + 1		
gen logit_surv = ln(rel / (1 - rel))		
gen source_MIR = "SEER"

export delimited using "${SEERdataOutput}\\neo_meso_SEER_5yr_MIRsurv.txt", replace

/////////////////////////////////
// These inc/mort/surv combined datasets will be used for survival prediction

// End of script
