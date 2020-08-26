
/////////////////////////////////////////////////////////
// Generating datasets for SEER-based MIRs, 5-year aggregates
/////////////////////////////////////////////////////////

// data downloaded via SEER*Stat:
// Rate Session: Mortality - All COD, Aggregated With State, Total U.S. (1969-2015) <Katrina/Rita Population Adjustment>
// Rate Session: Incidence - SEER 9 Regs Research Data, Nov 2016 Sub (1973-2014) <Katrina/Rita Population Adjustment>
// Survival Session: Incidence - SEER 9 Regs Research Data, Nov 2016 Sub (1973-2014) <Katrina/Rita Population Adjustment>


// Load Common Cancer Paths (sets global FILEPATH)
	run "FILEPATH" 


/////////////////////////////
// Format incidence datasets, 5-year aggregates
//
local cancers "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_meso neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"

set more off
local cancers "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"


foreach cancer of local cancers {
	import delimited "FILEPATH/`cancer'_SEER_inc_5years.txt"

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

	export delimited using "FILEPATH/`cancer'_SEERinc5_formatted.txt", replace
	
	save "FILEPATH/`cancer'_SEERinc5_formatted.dta", replace

	clear
	
}	



/////////////////////////////
// Format mortality datasets

local cancers "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_meso neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"

set more off
local cancers "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"


foreach cancer of local cancers {
	import delimited "FILEPATH/`cancer'_SEER_mort_5years.txt"

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

	export delimited using "FILEPATH/`cancer'_SEERmort5_formatted.txt", replace
	
	save "FILEPATH/`cancer'_SEERmort5_formatted.dta", replace

	clear
	
}	
	
	
/////////////////////////////
// Format survival datasets

local cancers "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_meso neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"

set more off
local cancers "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_meso neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"

foreach cancer of local cancers {
	import delimited "FILEPATH/`cancer'_SEER_surv_5years.txt"
	
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

	export delimited using "$FILEPATH/`cancer'_SEERsurv5_formatted.txt", replace
	
	save "FILEPATH/`cancer'_SEERsurv5_formatted.dta", replace

	clear
	
}	
		
//// use GBD estimates for neo_meso (since SEER doesn't report mortality)


import delimited FILEPATH/compiled_mir_vers_41.csv

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
rename mi_ratio old_mi_ratio
by year5 sex age_group_id: egen mi_ratio = mean(old_mi_ratio)
drop old_mi_ratio

drop if year > 1980 & year < 1985
drop if year > 1985 & year < 1990
drop if year > 1990 & year < 1995
drop if year > 1995 & year < 2000
drop if year > 2000 & year < 2005
drop if year > 2005 & year < 2010
drop if year > 2010 

save "FILEPATH/neo_meso_GBDmir5_formatted.dta", replace


	
//////////////////////////////
// Merging inc/mort datasets, then merging survival dataset

local cancers "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_meso neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"

set more off
local cancers "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"

foreach cancer of local cancers {	
	use "FILEPATH/`cancer'_SEERmort5_formatted.dta", clear
	
		merge 1:1 acause age_group_id year5 sex using "FILEPATH/`cancer'_SEERinc5_formatted.dta" , generate("`cancer'_Minc")
				
		gen mi_ratio = mort_crude_rate / inc_crude_rate
		gen invmir = 1 - mi_ratio
	
		merge 1:m acause age_group_id year5 sex using "FILEPATH/`cancer'_SEERsurv5_formatted.dta" , generate("`cancer'_Msurv")

		gen rel = relative / 100
		gen exp = expected / 100
		gen obs = expected / 100	
		
		gen source_MIR = "SEER"
		
		save "FILEPATH/`cancer'_SEER_5yr_MIRsurv.dta", replace
}
	
use "FILEPATH/neo_meso_GBDmir5_formatted.dta", clear
merge 1:m acause age_group_id year5 sex using "FILEPATH/neo_meso_SEERsurv5_formatted.dta" , generate("neo_meso_Msurv")
		
gen invmir = 1 - mi_ratio
gen rel = relative / 100
gen exp = expected / 100
gen obs = expected / 100	

// drop values from MIR dataset in younger/older age groups (since not in SEER)
drop if neo_meso_Msurv == 1  
		
gen source_MIR = "SEER"
		
save "FILEPATH/neo_meso_SEER_5yr_MIRsurv.dta", replace



/////////////////////////////////////////////////////////
// Diagnostic plots showing the relationship between SEER MIR and SEER survival
/////////////////////////////////////////////////////////

local cancers "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_meso neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"


global SEERplots "FILEPATH/SEER_mir_surv_5yr/"
global Xyearplots "FILEPATH/SEER_mir_surv_5yr/by_xyear/"
global AgeGroupPlots "FILEPATH/SEER_mir_surv_5yr/by_agegroup/"
global nPlots "FILEPATH/SEER_mir_surv_5yr/by_n/"
global yearsPlots "FILEPATH/SEER_mir_surv_5yr/by_years/"
global MIRsurvplots_linear "FILEPATH/SEER_mir_surv_pred_5yr/linear/"
global MIRsurvplots_poisson "FILEPATH/SEER_mir_surv_pred_5yr/poisson/"


local cancers "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_meso neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"
	
foreach cancer of local cancers {
	use "FILEPATH/`cancer'_SEER_5yr_MIRsurv.dta", clear
	
	gen interval = summaryinterval + 1
	
	gen include = 0
	replace include = 1 if year5 == "years_1980_1984"
	replace include = 1 if year5 == "years_1985_1989"
	replace include = 1 if year5 == "years_1990_1994"	
	
	foreach xyear of numlist 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 {
		xi: regress rel invmir if interval == `xyear' & n > 24 & include==1 [pweight=n]
		putexcel A`xyear'=("`cancer'") B`xyear'=("`xyear'-year survival") C`xyear'=(`e(N)') D`xyear'=(`e(rmse)') E`xyear'=(`e(r2_a)') F`xyear'=(`e(F)') G`xyear'=("`e(cmdline)'") using "FILEPATH/SEER_MIR_surv5yr.xlsx", sheet("`cancer'") modify
		putexcel A21=("Cancer") B21=("X-year survival") C21=("n") D21=("RMSE") E21=("Adj-R2") F21=("F statistic") G21=("Model specified") using "FILEPATH/SEER_MIR_surv5yr.xlsx", sheet("`cancer'") modify
		predict pred_`cancer'_`xyear' if interval == `xyear' 
		aaplot rel invmir if interval == `xyear' & n > 24 & include==1, title("SEER `cancer' `xyear'-year survival") ytitle("SEER `xyear'-year relative survival") xtitle("SEER (1 - MI ratio)") addplot(line rel rel)
		graph export "${SEERplots}//`cancer'_`xyear'_vs_MIR.png", replace
		aaplot rel pred_`cancer'_`xyear' if interval == `xyear' & n>24 & include==1, title("`cancer' `xyear'-year predicted survival, linear") ytitle("SEER `xyear'-relative survival") xtitle("Regression predicted survival") addplot(line rel rel)
		graph export "${MIRsurvplots_linear}//`cancer'_predicted_`xyear'_survival.png", replace
	}

	gen interval2 = summaryinterval + 23
	gen interval3 = summaryinterval + 101
	gen deathrate = 1 - rel
	
	foreach xyear2 of numlist 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 {
		poisson deathrate mi_ratio if interval2 == `xyear2' & n > 24 & include==1 [pweight=n]
		putexcel A`xyear2'=("`cancer'") B`xyear2'=("`xyear2'-22-year survival, Poisson") C`xyear2'=(`e(N)') D`xyear2'=(`e(ll)') E`xyear2'=(`e(r2_p)') F`xyear2'=(`e(chi2)') G`xyear2'=("`e(cmdline)'") using "FILEPATH/SEER_MIR_surv5yr.xlsx", sheet("`cancer'") modify
		putexcel A22=("Cancer") B22=("X-year survival") C22=("n") D22=("Log likelihood") E22=("Pseudo-R2") F22=("chi-sq statistic") G22=("Model specified") using "FILEPATH/SEER_MIR_surv5yr.xlsx", sheet("`cancer'") modify
		predict pred_`cancer'_`xyear2' if interval2 == `xyear2' 
		
		gen invpr_`cancer'_`xyear2' = 1 - pred_`cancer'_`xyear2'
		
		aaplot rel invpr_`cancer'_`xyear2' if interval2 == `xyear2' & n>24 & include==1, title("`cancer' `xyear2'-22-year predicted survival, Poisson") ytitle("SEER `xyear2'-relative survival") xtitle("Regression predicted survival") addplot(line rel rel)
		graph export "${MIRsurvplots_poisson}//`cancer'_predicted_`xyear2'_survival_poisson.png", replace
	}
	
	scatter rel invmir if age_group_id != 21, by(interval, title("`cancer' X-year survival") legend(off) note("Each dot = a sex/year/age_group combination; red = age 80+")) || scatter rel invmir if age_group_id == 21, by(interval) mc(red) || line rel rel ,  xtitle("SEER M/I Ratio") ytitle("SEER relative survival")
	graph export "${Xyearplots}//`cancer'_by_xyear.png", replace
	scatter rel invmir if interval!=5, by(age_group_id, title("`cancer' survival by GBD age group") legend(off) note("Each dot = a sex/year/survival_years combination; red = 5-year survival")) || scatter rel invmir if interval==5, by(age_group_id) mc(red) || line rel rel ,  xtitle("SEER M/I Ratio") ytitle("SEER relative survival")
	graph export "${AgeGroupPlots}//`cancer'_by_age_group.png", replace
	scatter rel invmir if n >= 25, by(interval, title("`cancer' X-year survival, n") legend(off) note("Each dot = a sex/year/age_group combination; red = n < 25")) || scatter rel invmir if n < 25, by(interval) mc(red) msymbol(Oh) || line rel rel ,  xtitle("SEER M/I Ratio") ytitle("SEER relative survival")
	graph export "${nPlots}//`cancer'_by_n.png", replace
	scatter rel invmir if include==1, by(interval, title("`cancer' X-year survival, years") legend(off) note("Each dot = a sex/year/survival_years combination; blue = 1980-1994, red = 1995-2014")) || scatter rel invmir if include==0, by(interval) mc(red) msymbol(Oh) || line rel rel ,  xtitle("SEER M/I Ratio") ytitle("SEER relative survival")
	graph export "${yearsPlots}//`cancer'_by_years.png", replace
	
	clear
}



///////////////////////////////////////////////////////
/// Merging GBD MIRs with SEER surv/MIRs, get dataset for each cancer
	
local cancers "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_meso neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"
	
set more off

import delimited "FILEPATH/cnf_compiled_mirs_version1.csv"

rename means mi_ratio
rename year_id year
rename sex_id sex
drop v1 
drop cause_id

gen invmir = 1 - mi_ratio
gen mi_ratio_GBD = mi_ratio
gen invmir_GBD = 1 - mi_ratio_GBD

gen source_MIR = "GBD"

local cancers "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_meso neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"

foreach cancer of local cancers {	
	preserve
		keep if acause == "`cancer'"
		
		append using "FILEPATH/`cancer'_SEER_5yr_MIRsurv.dta"
		
		save "FILEPATH/`cancer'_merged.dta", replace
	restore
}	

clear 


///////////////////////////////////////
/// Code for predicting survival

/// NOTE: this code is repeated several times, due to slight differences in how the cancers are predicted:
/// most cancers, male-specific cancers, female-specific cancers, rare cancers, and leukemia_other (uses AML values for worst-case scenario)

// get predicted estimates for the given X-year

// add floor to prediction, using 2016 5-year worst-case survival curves

set more off
local cancers "neo_bladder neo_brain neo_breast neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_liver neo_lung neo_lymphoma neo_melanoma neo_mouth neo_myeloma neo_nasopharynx neo_otherpharynx neo_other_cancer neo_pancreas neo_stomach neo_thyroid"


foreach cancer of local cancers {
	use "FILEPATH/`cancer'_merged.dta", replace
	
	merge m:1 acause sex using "FILEPATH/SEER_10year_survival_2004.dta"
	drop if _merge==2
	drop _merge
	
	gen interval = summaryinterval + 1
	gen deathrate = 1 - rel
	
	gen include = 0
	replace include = 1 if year5 == "years_1980_1984"
	replace include = 1 if year5 == "years_1985_1989"
	replace include = 1 if year5 == "years_1990_1994"	

	poisson deathrate mi_ratio if interval == 5 & n > 24 & source_MIR=="SEER" [pweight=n]
	predict pred_dr_`cancer' if source_MIR=="GBD"
	
	drop if source_MIR=="SEER"
	
	gen pred_x_`cancer' = 1 - pred_dr_`cancer' if source_MIR=="GBD"
	
	gen pred_`cancer' = pred_x_`cancer'
	replace pred_`cancer' = 0 if pred_x_`cancer' < 0 & source_MIR=="GBD"
	replace pred_`cancer' = 1 if pred_x_`cancer' > 1 & source_MIR=="GBD"

	merge m:1 sex using "FILEPATH/worst_case_survival_wide.dta"
	replace pred_`cancer' = worst_`cancer' if pred_x_`cancer' < worst_`cancer' & source_MIR=="GBD"
		
	gen prop_haz_scalar = pred_`cancer' / surv_5year
	
	gen scaled_1year = prop_haz_scalar * surv_1year
	gen scaled_2year = prop_haz_scalar * surv_2year
	gen scaled_3year = prop_haz_scalar * surv_3year
	gen scaled_4year = prop_haz_scalar * surv_4year
	gen scaled_5year = prop_haz_scalar * surv_5year
	gen scaled_6year = prop_haz_scalar * surv_6year
	gen scaled_7year = prop_haz_scalar * surv_7year
	gen scaled_8year = prop_haz_scalar * surv_8year
	gen scaled_9year = prop_haz_scalar * surv_9year
	gen scaled_10year = prop_haz_scalar * surv_10year
	
	gen scaled_10year_restrict = scaled_10year
	replace scaled_10year_restrict = 0 if scaled_10year < 0
	
	gen surv_diff = surv_10year - scaled_10year
	gen under_zero = 1 if scaled_10year < 0
	gen overUSA = 1 if scaled_10year > surv_10year	
	egen max_seer_m = max(surv_10year) if sex == 1
	egen max_seer_f = max(surv_10year) if sex == 2
	gen scaled_10year_restrict2 = scaled_10year_restrict
	replace scaled_10year_restrict2 = max_seer_m if scaled_10year_restrict2 > max_seer_m & sex == 1	
	replace scaled_10year_restrict2 = max_seer_f if scaled_10year_restrict2 > max_seer_f & sex == 2
	
	gen rowcounter = sex + 1
	gen rowcounter2 = sex + 4	
	gen rowcounter3 = sex + 7
	gen rowcounter4 = sex + 10
	
	putexcel C1=("min_10year") D1=("25th_10year") E1=("50th_10year") F1=("75th_10year") G1=("max_10year") H1=("sd_10year") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel L1=("min_10year_diff") M1=("25th_10year_diff") N1=("50th_10year_diff") O1=("75th_10year_diff") P1=("max_10year_diff") Q1=("sd_10year_diff") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel A2=("Predicted_survival") A3=("Predicted_survival") A5=("Restricted_survival") A6=("Restricted_survival") A8=("Restricted_survival_both") A9=("Restricted_survival_both") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel B1=("Sex") B2=("Male") B3=("Female") B5=("Male") B6=("Female") B8=("Male") B9=("Female") J1=("SEER_10year") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel A11=("Obs_pred<0_males") A12=("Obs_pred<0_females") C11=("Obs_pred>SEER_males") C12=("Obs_pred>SEER_females") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify

	foreach counter of numlist 2 3 {
		quietly sum scaled_10year if rowcounter == `counter', detail		
		putexcel C`counter'=(`r(min)') D`counter'=(`r(p25)') E`counter'=(`r(p50)') F`counter'=(`r(p75)') G`counter'=(`r(max)') H`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
		quietly sum surv_diff if rowcounter == `counter', detail
		putexcel L`counter'=(`r(min)') M`counter'=(`r(p25)') N`counter'=(`r(p50)') O`counter'=(`r(p75)') P`counter'=(`r(max)') Q`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
		quietly sum surv_10year if rowcounter == `counter'
		putexcel J`counter'=(`r(max)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}
	
	foreach counter of numlist 5 6 {
		quietly sum scaled_10year_restrict if rowcounter2 == `counter', detail		
		putexcel C`counter'=(`r(min)') D`counter'=(`r(p25)') E`counter'=(`r(p50)') F`counter'=(`r(p75)') G`counter'=(`r(max)') H`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}

	foreach counter of numlist 8 9 {
		quietly sum scaled_10year_restrict2 if rowcounter3 == `counter', detail		
		putexcel C`counter'=(`r(min)') D`counter'=(`r(p25)') E`counter'=(`r(p50)') F`counter'=(`r(p75)') G`counter'=(`r(max)') H`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}
	
	foreach counter of numlist 11 12 {
		quietly tab under_zero if rowcounter4 == `counter'
		putexcel B`counter'=(`r(N)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
		quietly tab overUSA if rowcounter4 == `counter'
		putexcel D`counter'=(`r(N)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}

	save "FILEPATH/`cancer'_predicted_survival.dta", replace
}	
	
////// For sex-specific cancers (Male): prostate, testicular

local cancers "neo_prostate neo_testicular"	
set more off
foreach cancer of local cancers {
	use "${FILEPATH/`cancer'_merged.dta", replace
	
	merge m:1 acause sex using "FILEPATH/SEER_10year_survival_2004.dta"
	drop if _merge==2
	drop _merge
	
	gen interval = summaryinterval + 1
	gen deathrate = 1 - rel
	
	gen include = 0
	replace include = 1 if year5 == "years_1980_1984"
	replace include = 1 if year5 == "years_1985_1989"
	replace include = 1 if year5 == "years_1990_1994"	

	poisson deathrate mi_ratio if interval == 5 & n > 24 & source_MIR=="SEER" [pweight=n]
	predict pred_dr_`cancer' if source_MIR=="GBD"
	
	drop if source_MIR=="SEER"
	
	gen pred_x_`cancer' = 1 - pred_dr_`cancer' if source_MIR=="GBD"
	
	gen pred_`cancer' = pred_x_`cancer'
	replace pred_`cancer' = 0 if pred_x_`cancer' < 0 & source_MIR=="GBD"
	replace pred_`cancer' = 1 if pred_x_`cancer' > 1 & source_MIR=="GBD"
	
	merge m:1 sex using "FILEPATH/worst_case_survival_wide.dta"
	replace pred_`cancer' = worst_`cancer' if pred_x_`cancer' < worst_`cancer' & source_MIR=="GBD"
	
	gen prop_haz_scalar = pred_`cancer' / surv_5year
	
	gen scaled_1year = prop_haz_scalar * surv_1year
	gen scaled_2year = prop_haz_scalar * surv_2year
	gen scaled_3year = prop_haz_scalar * surv_3year
	gen scaled_4year = prop_haz_scalar * surv_4year
	gen scaled_5year = prop_haz_scalar * surv_5year
	gen scaled_6year = prop_haz_scalar * surv_6year
	gen scaled_7year = prop_haz_scalar * surv_7year
	gen scaled_8year = prop_haz_scalar * surv_8year
	gen scaled_9year = prop_haz_scalar * surv_9year
	gen scaled_10year = prop_haz_scalar * surv_10year
	
	gen scaled_10year_restrict = scaled_10year
	replace scaled_10year_restrict = 0 if scaled_10year < 0
	
	gen surv_diff = surv_10year - scaled_10year
	gen under_zero = 1 if scaled_10year < 0
	gen overUSA = 1 if scaled_10year > surv_10year	
	egen max_seer_m = max(surv_10year) if sex == 1
	egen max_seer_f = max(surv_10year) if sex == 2
	gen scaled_10year_restrict2 = scaled_10year_restrict
	replace scaled_10year_restrict2 = max_seer_m if scaled_10year_restrict2 > max_seer_m & sex == 1	
	replace scaled_10year_restrict2 = max_seer_f if scaled_10year_restrict2 > max_seer_f & sex == 2
	
	gen rowcounter = sex + 1
	gen rowcounter2 = sex + 4	
	gen rowcounter3 = sex + 7
	gen rowcounter4 = sex + 10
	
	putexcel C1=("min_10year") D1=("25th_10year") E1=("50th_10year") F1=("75th_10year") G1=("max_10year") H1=("sd_10year") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel L1=("min_10year_diff") M1=("25th_10year_diff") N1=("50th_10year_diff") O1=("75th_10year_diff") P1=("max_10year_diff") Q1=("sd_10year_diff") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel A2=("Predicted_survival") A3=("Predicted_survival") A5=("Restricted_survival") A6=("Restricted_survival") A8=("Restricted_survival_both") A9=("Restricted_survival_both") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel B1=("Sex") B2=("Male") B3=("Female") B5=("Male") B6=("Female") B8=("Male") B9=("Female") J1=("SEER_10year") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel A11=("Obs_pred<0_males") A12=("Obs_pred<0_females") C11=("Obs_pred>SEER_males") C12=("Obs_pred>SEER_females") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify

	foreach counter of numlist 2 {
		quietly sum scaled_10year if rowcounter == `counter', detail		
		putexcel C`counter'=(`r(min)') D`counter'=(`r(p25)') E`counter'=(`r(p50)') F`counter'=(`r(p75)') G`counter'=(`r(max)') H`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
		quietly sum surv_diff if rowcounter == `counter', detail
		putexcel L`counter'=(`r(min)') M`counter'=(`r(p25)') N`counter'=(`r(p50)') O`counter'=(`r(p75)') P`counter'=(`r(max)') Q`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
		quietly sum surv_10year if rowcounter == `counter'
		putexcel J`counter'=(`r(max)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}
	
	foreach counter of numlist 5 {
		quietly sum scaled_10year_restrict if rowcounter2 == `counter', detail		
		putexcel C`counter'=(`r(min)') D`counter'=(`r(p25)') E`counter'=(`r(p50)') F`counter'=(`r(p75)') G`counter'=(`r(max)') H`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}

	foreach counter of numlist 8 {
		quietly sum scaled_10year_restrict2 if rowcounter3 == `counter', detail		
		putexcel C`counter'=(`r(min)') D`counter'=(`r(p25)') E`counter'=(`r(p50)') F`counter'=(`r(p75)') G`counter'=(`r(max)') H`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}
	
	foreach counter of numlist 11 {
		quietly tab under_zero if rowcounter4 == `counter'
		putexcel B`counter'=(`r(N)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
		quietly tab overUSA if rowcounter4 == `counter'
		putexcel D`counter'=(`r(N)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}

	save "FILEPATH/`cancer'_predicted_survival.dta", replace
}	
	


///// For sex-specific cancers (Female): cervical, ovarian, uterine

local cancers "neo_cervical neo_ovarian neo_uterine"	
set more off
foreach cancer of local cancers {
	use "FILEPATH/`cancer'_merged.dta", replace
	
	merge m:1 acause sex using "FILEPATH/SEER_10year_survival_2004.dta"
	drop if _merge==2
	drop _merge
	
	gen interval = summaryinterval + 1
	gen deathrate = 1 - rel
	
	gen include = 0
	replace include = 1 if year5 == "years_1980_1984"
	replace include = 1 if year5 == "years_1985_1989"
	replace include = 1 if year5 == "years_1990_1994"	

	poisson deathrate mi_ratio if interval == 5 & n > 24 & source_MIR=="SEER" [pweight=n]
	predict pred_dr_`cancer' if source_MIR=="GBD"
	
	drop if source_MIR=="SEER"
	
	gen pred_x_`cancer' = 1 - pred_dr_`cancer' if source_MIR=="GBD"
	
	gen pred_`cancer' = pred_x_`cancer'
	replace pred_`cancer' = 0 if pred_x_`cancer' < 0 & source_MIR=="GBD"
	replace pred_`cancer' = 1 if pred_x_`cancer' > 1 & source_MIR=="GBD"
	
	merge m:1 sex using "FILEPATH/worst_case_survival_wide.dta"
	replace pred_`cancer' = worst_`cancer' if pred_x_`cancer' < worst_`cancer' & source_MIR=="GBD"
		
	gen prop_haz_scalar = pred_`cancer' / surv_5year
	
	gen scaled_1year = prop_haz_scalar * surv_1year
	gen scaled_2year = prop_haz_scalar * surv_2year
	gen scaled_3year = prop_haz_scalar * surv_3year
	gen scaled_4year = prop_haz_scalar * surv_4year
	gen scaled_5year = prop_haz_scalar * surv_5year
	gen scaled_6year = prop_haz_scalar * surv_6year
	gen scaled_7year = prop_haz_scalar * surv_7year
	gen scaled_8year = prop_haz_scalar * surv_8year
	gen scaled_9year = prop_haz_scalar * surv_9year
	gen scaled_10year = prop_haz_scalar * surv_10year
	
	gen scaled_10year_restrict = scaled_10year
	replace scaled_10year_restrict = 0 if scaled_10year < 0
	
	gen surv_diff = surv_10year - scaled_10year
	gen under_zero = 1 if scaled_10year < 0
	gen overUSA = 1 if scaled_10year > surv_10year	
	egen max_seer_m = max(surv_10year) if sex == 1
	egen max_seer_f = max(surv_10year) if sex == 2
	gen scaled_10year_restrict2 = scaled_10year_restrict
	replace scaled_10year_restrict2 = max_seer_m if scaled_10year_restrict2 > max_seer_m & sex == 1	
	replace scaled_10year_restrict2 = max_seer_f if scaled_10year_restrict2 > max_seer_f & sex == 2
	
	gen rowcounter = sex + 1
	gen rowcounter2 = sex + 4	
	gen rowcounter3 = sex + 7
	gen rowcounter4 = sex + 10
	
	putexcel C1=("min_10year") D1=("25th_10year") E1=("50th_10year") F1=("75th_10year") G1=("max_10year") H1=("sd_10year") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel L1=("min_10year_diff") M1=("25th_10year_diff") N1=("50th_10year_diff") O1=("75th_10year_diff") P1=("max_10year_diff") Q1=("sd_10year_diff") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel A2=("Predicted_survival") A3=("Predicted_survival") A5=("Restricted_survival") A6=("Restricted_survival") A8=("Restricted_survival_both") A9=("Restricted_survival_both") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel B1=("Sex") B2=("Male") B3=("Female") B5=("Male") B6=("Female") B8=("Male") B9=("Female") J1=("SEER_10year") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel A11=("Obs_pred<0_males") A12=("Obs_pred<0_females") C11=("Obs_pred>SEER_males") C12=("Obs_pred>SEER_females") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify

	foreach counter of numlist 3 {
		quietly sum scaled_10year if rowcounter == `counter', detail		
		putexcel C`counter'=(`r(min)') D`counter'=(`r(p25)') E`counter'=(`r(p50)') F`counter'=(`r(p75)') G`counter'=(`r(max)') H`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
		quietly sum surv_diff if rowcounter == `counter', detail
		putexcel L`counter'=(`r(min)') M`counter'=(`r(p25)') N`counter'=(`r(p50)') O`counter'=(`r(p75)') P`counter'=(`r(max)') Q`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
		quietly sum surv_10year if rowcounter == `counter'
		putexcel J`counter'=(`r(max)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}
	
	foreach counter of numlist 6 {
		quietly sum scaled_10year_restrict if rowcounter2 == `counter', detail		
		putexcel C`counter'=(`r(min)') D`counter'=(`r(p25)') E`counter'=(`r(p50)') F`counter'=(`r(p75)') G`counter'=(`r(max)') H`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}

	foreach counter of numlist 9 {
		quietly sum scaled_10year_restrict2 if rowcounter3 == `counter', detail		
		putexcel C`counter'=(`r(min)') D`counter'=(`r(p25)') E`counter'=(`r(p50)') F`counter'=(`r(p75)') G`counter'=(`r(max)') H`counter'=(`r(sd)') using "FILEPATH/Diagnostics/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}
	
	foreach counter of numlist 12 {
		quietly tab under_zero if rowcounter4 == `counter'
		putexcel B`counter'=(`r(N)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
		quietly tab overUSA if rowcounter4 == `counter'
		putexcel D`counter'=(`r(N)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}

	save "FILEPATH/`cancer'_predicted_survival.dta", replace
}	
	
	

	
///// For rarer cancers: nasopharynx, ALL, meso

local cancers "neo_meso neo_nasopharynx neo_leukemia_ll_acute"	
set more off
foreach cancer of local cancers {
	use "FILEPATH/`cancer'_merged.dta", replace
	
	merge m:1 acause sex using "FILEPATH/SEER_10year_survival_2004.dta"
	drop if _merge==2
	drop _merge
	
	gen interval = summaryinterval + 1
	gen deathrate = 1 - rel
	
	gen include = 0
	replace include = 1 if year5 == "years_1980_1984"
	replace include = 1 if year5 == "years_1985_1989"
	replace include = 1 if year5 == "years_1990_1994"	

	poisson deathrate mi_ratio if interval == 5 & n > 9 & source_MIR=="SEER" [pweight=n]
	predict pred_dr_`cancer' if source_MIR=="GBD"
	
	drop if source_MIR=="SEER"
	
	gen pred_x_`cancer' = 1 - pred_dr_`cancer' if source_MIR=="GBD"
	
	gen pred_`cancer' = pred_x_`cancer'
	replace pred_`cancer' = 0 if pred_x_`cancer' < 0 & source_MIR=="GBD"
	replace pred_`cancer' = 1 if pred_x_`cancer' > 1 & source_MIR=="GBD"
	
	merge m:1 sex using "FILEPATH/worst_case_survival_wide.dta"
	replace pred_`cancer' = worst_`cancer' if pred_x_`cancer' < worst_`cancer' & source_MIR=="GBD"
		
	gen prop_haz_scalar = pred_`cancer' / surv_5year
	
	gen scaled_1year = prop_haz_scalar * surv_1year
	gen scaled_2year = prop_haz_scalar * surv_2year
	gen scaled_3year = prop_haz_scalar * surv_3year
	gen scaled_4year = prop_haz_scalar * surv_4year
	gen scaled_5year = prop_haz_scalar * surv_5year
	gen scaled_6year = prop_haz_scalar * surv_6year
	gen scaled_7year = prop_haz_scalar * surv_7year
	gen scaled_8year = prop_haz_scalar * surv_8year
	gen scaled_9year = prop_haz_scalar * surv_9year
	gen scaled_10year = prop_haz_scalar * surv_10year
	
	gen scaled_10year_restrict = scaled_10year
	replace scaled_10year_restrict = 0 if scaled_10year < 0
	
	gen surv_diff = surv_10year - scaled_10year
	gen under_zero = 1 if scaled_10year < 0
	gen overUSA = 1 if scaled_10year > surv_10year	
	egen max_seer_m = max(surv_10year) if sex == 1
	egen max_seer_f = max(surv_10year) if sex == 2
	gen scaled_10year_restrict2 = scaled_10year_restrict
	replace scaled_10year_restrict2 = max_seer_m if scaled_10year_restrict2 > max_seer_m & sex == 1	
	replace scaled_10year_restrict2 = max_seer_f if scaled_10year_restrict2 > max_seer_f & sex == 2
	
	gen rowcounter = sex + 1
	gen rowcounter2 = sex + 4	
	gen rowcounter3 = sex + 7
	gen rowcounter4 = sex + 10
	
	putexcel C1=("min_10year") D1=("25th_10year") E1=("50th_10year") F1=("75th_10year") G1=("max_10year") H1=("sd_10year") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel L1=("min_10year_diff") M1=("25th_10year_diff") N1=("50th_10year_diff") O1=("75th_10year_diff") P1=("max_10year_diff") Q1=("sd_10year_diff") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel A2=("Predicted_survival") A3=("Predicted_survival") A5=("Restricted_survival") A6=("Restricted_survival") A8=("Restricted_survival_both") A9=("Restricted_survival_both") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel B1=("Sex") B2=("Male") B3=("Female") B5=("Male") B6=("Female") B8=("Male") B9=("Female") J1=("SEER_10year") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel A11=("Obs_pred<0_males") A12=("Obs_pred<0_females") C11=("Obs_pred>SEER_males") C12=("Obs_pred>SEER_females") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify

	foreach counter of numlist 2 3 {
		quietly sum scaled_10year if rowcounter == `counter', detail		
		putexcel C`counter'=(`r(min)') D`counter'=(`r(p25)') E`counter'=(`r(p50)') F`counter'=(`r(p75)') G`counter'=(`r(max)') H`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
		quietly sum surv_diff if rowcounter == `counter', detail
		putexcel L`counter'=(`r(min)') M`counter'=(`r(p25)') N`counter'=(`r(p50)') O`counter'=(`r(p75)') P`counter'=(`r(max)') Q`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
		quietly sum surv_10year if rowcounter == `counter'
		putexcel J`counter'=(`r(max)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}
	
	foreach counter of numlist 5 6 {
		quietly sum scaled_10year_restrict if rowcounter2 == `counter', detail		
		putexcel C`counter'=(`r(min)') D`counter'=(`r(p25)') E`counter'=(`r(p50)') F`counter'=(`r(p75)') G`counter'=(`r(max)') H`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}

	foreach counter of numlist 8 9 {
		quietly sum scaled_10year_restrict2 if rowcounter3 == `counter', detail		
		putexcel C`counter'=(`r(min)') D`counter'=(`r(p25)') E`counter'=(`r(p50)') F`counter'=(`r(p75)') G`counter'=(`r(max)') H`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}
	
	foreach counter of numlist 11 12 {
		quietly tab under_zero if rowcounter4 == `counter'
		putexcel B`counter'=(`r(N)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
		quietly tab overUSA if rowcounter4 == `counter'
		putexcel D`counter'=(`r(N)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}

	save "FILEPATH/`cancer'_predicted_survival.dta", replace
}	
	
	
	

	
// Need to use the GBD MIR for neo_mesothelioma, as SEER doesn't report mortality
	

// Doing neo_leukemia_other separately, so that can use AML as the worst-case scenario
// (changed comparison within code so not pulling from same name)
	
set more off
local cancers "neo_leukemia_other"


foreach cancer of local cancers {
	use "FILEPATH/`cancer'_merged.dta", replace
	
	merge m:1 acause sex using "FILEPATH/SEER_10year_survival_2004.dta"
	drop if _merge==2
	drop _merge
	
	gen interval = summaryinterval + 1
	gen deathrate = 1 - rel
	
	gen include = 0
	replace include = 1 if year5 == "years_1980_1984"
	replace include = 1 if year5 == "years_1985_1989"
	replace include = 1 if year5 == "years_1990_1994"	

	poisson deathrate mi_ratio if interval == 5 & n > 24 & source_MIR=="SEER" [pweight=n]
	predict pred_dr_`cancer' if source_MIR=="GBD"
	
	drop if source_MIR=="SEER"
	
	gen pred_x_`cancer' = 1 - pred_dr_`cancer' if source_MIR=="GBD"
	
	gen pred_`cancer' = pred_x_`cancer'
	replace pred_`cancer' = 0 if pred_x_`cancer' < 0 & source_MIR=="GBD"
	replace pred_`cancer' = 1 if pred_x_`cancer' > 1 & source_MIR=="GBD"

	merge m:1 sex using "FILEPATH/worst_case_survival_wide.dta"
	replace pred_`cancer' = worst_neo_leukemia_ml_acute if pred_x_`cancer' < worst_neo_leukemia_ml_acute & source_MIR=="GBD"
		
	gen prop_haz_scalar = pred_`cancer' / surv_5year
	
	gen scaled_1year = prop_haz_scalar * surv_1year
	gen scaled_2year = prop_haz_scalar * surv_2year
	gen scaled_3year = prop_haz_scalar * surv_3year
	gen scaled_4year = prop_haz_scalar * surv_4year
	gen scaled_5year = prop_haz_scalar * surv_5year
	gen scaled_6year = prop_haz_scalar * surv_6year
	gen scaled_7year = prop_haz_scalar * surv_7year
	gen scaled_8year = prop_haz_scalar * surv_8year
	gen scaled_9year = prop_haz_scalar * surv_9year
	gen scaled_10year = prop_haz_scalar * surv_10year
	
	gen scaled_10year_restrict = scaled_10year
	replace scaled_10year_restrict = 0 if scaled_10year < 0
	
	gen surv_diff = surv_10year - scaled_10year
	gen under_zero = 1 if scaled_10year < 0
	gen overUSA = 1 if scaled_10year > surv_10year	
	egen max_seer_m = max(surv_10year) if sex == 1
	egen max_seer_f = max(surv_10year) if sex == 2
	gen scaled_10year_restrict2 = scaled_10year_restrict
	replace scaled_10year_restrict2 = max_seer_m if scaled_10year_restrict2 > max_seer_m & sex == 1	
	replace scaled_10year_restrict2 = max_seer_f if scaled_10year_restrict2 > max_seer_f & sex == 2
	
	gen rowcounter = sex + 1
	gen rowcounter2 = sex + 4	
	gen rowcounter3 = sex + 7
	gen rowcounter4 = sex + 10
	
	putexcel C1=("min_10year") D1=("25th_10year") E1=("50th_10year") F1=("75th_10year") G1=("max_10year") H1=("sd_10year") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel L1=("min_10year_diff") M1=("25th_10year_diff") N1=("50th_10year_diff") O1=("75th_10year_diff") P1=("max_10year_diff") Q1=("sd_10year_diff") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel A2=("Predicted_survival") A3=("Predicted_survival") A5=("Restricted_survival") A6=("Restricted_survival") A8=("Restricted_survival_both") A9=("Restricted_survival_both") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel B1=("Sex") B2=("Male") B3=("Female") B5=("Male") B6=("Female") B8=("Male") B9=("Female") J1=("SEER_10year") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	putexcel A11=("Obs_pred<0_males") A12=("Obs_pred<0_females") C11=("Obs_pred>SEER_males") C12=("Obs_pred>SEER_females") using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify

	foreach counter of numlist 2 3 {
		quietly sum scaled_10year if rowcounter == `counter', detail		
		putexcel C`counter'=(`r(min)') D`counter'=(`r(p25)') E`counter'=(`r(p50)') F`counter'=(`r(p75)') G`counter'=(`r(max)') H`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
		quietly sum surv_diff if rowcounter == `counter', detail
		putexcel L`counter'=(`r(min)') M`counter'=(`r(p25)') N`counter'=(`r(p50)') O`counter'=(`r(p75)') P`counter'=(`r(max)') Q`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
		quietly sum surv_10year if rowcounter == `counter'
		putexcel J`counter'=(`r(max)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}
	
	foreach counter of numlist 5 6 {
		quietly sum scaled_10year_restrict if rowcounter2 == `counter', detail		
		putexcel C`counter'=(`r(min)') D`counter'=(`r(p25)') E`counter'=(`r(p50)') F`counter'=(`r(p75)') G`counter'=(`r(max)') H`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}

	foreach counter of numlist 8 9 {
		quietly sum scaled_10year_restrict2 if rowcounter3 == `counter', detail		
		putexcel C`counter'=(`r(min)') D`counter'=(`r(p25)') E`counter'=(`r(p50)') F`counter'=(`r(p75)') G`counter'=(`r(max)') H`counter'=(`r(sd)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}
	
	foreach counter of numlist 11 12 {
		quietly tab under_zero if rowcounter4 == `counter'
		putexcel B`counter'=(`r(N)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
		quietly tab overUSA if rowcounter4 == `counter'
		putexcel D`counter'=(`r(N)') using "FILEPATH/GBD_predictions.xlsx", sheet("`cancer'_pred") modify
	}

	save "FILEPATH/`cancer'_predicted_survival.dta", replace
}		
	
	
	
	
///////////////////////////////////////////////
// Code for merging and saving predicted survival into location-specific datasets
// ////////////////////////////////////////////

set more off
local cancers "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_meso neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"

foreach cancer in `cancers' {
	use "FILEPATH/`cancer'_predicted_survival.dta", clear
		noi di "`cancer'"
		
		forvalues drop_dups = 1/19 {
			drop if summaryinterval == `drop_dups'
		}
		
		drop invmir-prop_haz_scalar
		drop surv_diff-rowcounter4
		
		forvalues restrict = 1/10 {
			replace scaled_`restrict'year = 0 if scaled_`restrict'year < 0 & scaled_`restrict'year != .
			replace scaled_`restrict'year = 1 if scaled_`restrict'year > 1 & scaled_`restrict'year != .
		}
		
		levelsof location_id, local(locations) c

		tempfile master_data
		save `master_data'
		
		foreach l of local locations {
			use `master_data' if location_id == `l', clear
				
			sum location_id, meanonly
			local location_name `r(max)'				

			export delimited "FILEPATH/pred_surv_by_location/`cancer'/`l'.csv", replace

		}
}	
	
	
/////////////////////////////////////////////////////
////// comparisons to CONCORD ///////////////////////
/////////////////////////////////////////////////////

local cancers "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"


////// for cancers with distribution 1: stomach, colorectal, liver, lung, breast, ovary, prostate, leukemia (note leukemia is all leukemias in CONCORD)

set more off
global CONCORDplots "FILEPATH"
local cancers "neo_stomach neo_colorectal neo_liver neo_lung neo_breast neo_ovarian neo_prostate neo_leukemia_ll_chronic"

foreach cancer of local cancers {

	use "FILEPATH/`cancer'_predicted_survival.dta", clear

	gen age_spec_5year = .
	replace age_spec_5year = scaled_5year * 0.0016 if age_group_id == 8
	replace age_spec_5year = scaled_5year * 0.0021 if age_group_id == 9
	replace age_spec_5year = scaled_5year * 0.0042 if age_group_id == 10
	replace age_spec_5year = scaled_5year * 0.0084 if age_group_id == 11
	replace age_spec_5year = scaled_5year * 0.0188 if age_group_id == 12
	replace age_spec_5year = scaled_5year * 0.0349 if age_group_id == 13
	replace age_spec_5year = scaled_5year * 0.0491 if age_group_id == 14
	replace age_spec_5year = scaled_5year * 0.0709 if age_group_id == 15
	replace age_spec_5year = scaled_5year * 0.0964 if age_group_id == 16
	replace age_spec_5year = scaled_5year * 0.1336 if age_group_id == 17
	replace age_spec_5year = scaled_5year * 0.1423 if age_group_id == 18
	replace age_spec_5year = scaled_5year * 0.1477 if age_group_id == 19
	replace age_spec_5year = scaled_5year * 0.1413 if age_group_id == 20
	replace age_spec_5year = scaled_5year * 0.1487 if age_group_id == 21
	
	sort location_id sex year
	
	by location_id sex year: egen age_std = sum(age_spec_5year )
	
	keep if age_group_id == 1
	
	forvalues drop_year = 1980/1996 {
		drop if year == `drop_year'
	}	
	forvalues drop_year = 1998/2001 {
		drop if year == `drop_year'
	}	
	forvalues drop_year = 2003/2006 {
		drop if year == `drop_year'
	}	
	forvalues drop_year =  2008/2017 {
		drop if year == `drop_year'
	}	
	
	merge m:1 acause year location_id using "FILEPATH/CONCORD_for_merging.dta"
	
	replace concord_survival = concord_survival / 100
	
	scatter concord_survival age_std , by(sex year, title("`cancer' GBD vs. CONCORD age-standardized survival")  legend(off) note("red=1997, green=2002, blue=2007"))  || line concord_survival concord_survival ,  xtitle("GBD predicted survival") ytitle("CONCORD relative survival") xscale(range(0 1)) yscale(range(0 1))
	graph export "${CONCORDplots}//`cancer'_CONCORD_GBD.png", replace
	

}	


////// for cancers with distribution 2: cervical

set more off
global CONCORDplots "FILEPATH"
local cancers "neo_cervical"

foreach cancer of local cancers {

	use "FILEPATH/`cancer'_predicted_survival.dta", clear

	gen age_spec_5year = .
	replace age_spec_5year = scaled_5year * 0.0113 if age_group_id == 8
	replace age_spec_5year = scaled_5year * 0.0201 if age_group_id == 9
	replace age_spec_5year = scaled_5year * 0.0374 if age_group_id == 10
	replace age_spec_5year = scaled_5year * 0.0575 if age_group_id == 11
	replace age_spec_5year = scaled_5year * 0.0754 if age_group_id == 12
	replace age_spec_5year = scaled_5year * 0.0782 if age_group_id == 13
	replace age_spec_5year = scaled_5year * 0.0828 if age_group_id == 14
	replace age_spec_5year = scaled_5year * 0.0872 if age_group_id == 15
	replace age_spec_5year = scaled_5year * 0.0990 if age_group_id == 16
	replace age_spec_5year = scaled_5year * 0.1110 if age_group_id == 17
	replace age_spec_5year = scaled_5year * 0.1100 if age_group_id == 18
	replace age_spec_5year = scaled_5year * 0.0900 if age_group_id == 19
	replace age_spec_5year = scaled_5year * 0.0725 if age_group_id == 20
	replace age_spec_5year = scaled_5year * 0.0675 if age_group_id == 21
	
	sort location_id sex year
	
	by location_id sex year: egen age_std = sum(age_spec_5year )
	
	keep if age_group_id == 1
	
	forvalues drop_year = 1980/1996 {
		drop if year == `drop_year'
	}	
	forvalues drop_year = 1998/2001 {
		drop if year == `drop_year'
	}	
	forvalues drop_year = 2003/2006 {
		drop if year == `drop_year'
	}	
	forvalues drop_year =  2008/2017 {
		drop if year == `drop_year'
	}	
	
	merge m:1 acause year location_id using "FILEPATH/CONCORD_for_merging.dta"
	
	replace concord_survival = concord_survival / 100
	
	scatter concord_survival age_std , by(sex year, title("`cancer' GBD vs. CONCORD age-standardized survival")  legend(off) note("red=1997, green=2002, blue=2007"))  || line concord_survival concord_survival ,  xtitle("GBD predicted survival") ytitle("CONCORD relative survival") xscale(range(0 1)) yscale(range(0 1))
	graph export "${CONCORDplots}//`cancer'_CONCORD_GBD.png", replace
	

}	


/////


////// for cancers with childhood distribution: ALL (note this data is children in CONCORD, but all ages in GBD)

set more off
global CONCORDplots "FILEPATH"
local cancers "neo_leukemia_ll_acute"

foreach cancer of local cancers {

	use "FILEPATH/`cancer'_predicted_survival.dta", clear

	gen age_spec_5year = .
	replace age_spec_5year = scaled_5year * 0.3333 if age_group_id == 1
	replace age_spec_5year = scaled_5year * 0.3333 if age_group_id == 6
	replace age_spec_5year = scaled_5year * 0.3333 if age_group_id == 7
	
	sort location_id sex year
	
	by location_id sex year: egen age_std = sum(age_spec_5year )
	
	keep if age_group_id == 1
	
	forvalues drop_year = 1980/1996 {
		drop if year == `drop_year'
	}	
	forvalues drop_year = 1998/2001 {
		drop if year == `drop_year'
	}	
	forvalues drop_year = 2003/2006 {
		drop if year == `drop_year'
	}	
	forvalues drop_year =  2008/2017 {
		drop if year == `drop_year'
	}	
	
	merge m:1 acause year location_id using "FILEPATH/CONCORD_for_merging.dta"
	
	replace concord_survival = concord_survival / 100
	
	scatter concord_survival age_std , by(sex year, title("`cancer' GBD vs. CONCORD age-standardized survival")  legend(off) note("red=1997, green=2002, blue=2007"))  || line concord_survival concord_survival ,  xtitle("GBD predicted survival") ytitle("CONCORD relative survival") xscale(range(0 1)) yscale(range(0 1))
	graph export "${CONCORDplots}//`cancer'_CONCORD_GBD.png", replace
	

}	





//////



/////////////////////////////////////////////////////
////// comparisons to EUROCARE //////////////////////
/////////////////////////////////////////////////////

local cancers "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"


////// for cancers with distribution 1a: All other sites except prostate
** NOTE that the EUROCARE age groups are 15-44, 45-54, 55-64, 65-74, 75-100
** NOTE that the EUROCARE ICSS weights are the larger age groups, while the GBD weights are from 5-year age groups (these weights sum to the weights of the larger age groups, by distribution)

set more off

global EUROCAREplots "$FILEPATH"

local cancers "neo_bladder neo_colorectal neo_esophageal neo_gallbladder neo_kidney neo_larynx neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_liver neo_lung neo_lymphoma neo_mouth neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach"

foreach cancer of local cancers {

	use "$FILEPATH/`cancer'_predicted_survival.dta", clear

	gen age_spec_5year = .
	replace age_spec_5year = scaled_5year * 0.0016 if age_group_id == 8
	replace age_spec_5year = scaled_5year * 0.0021 if age_group_id == 9
	replace age_spec_5year = scaled_5year * 0.0042 if age_group_id == 10
	replace age_spec_5year = scaled_5year * 0.0084 if age_group_id == 11
	replace age_spec_5year = scaled_5year * 0.0188 if age_group_id == 12
	replace age_spec_5year = scaled_5year * 0.0349 if age_group_id == 13
	replace age_spec_5year = scaled_5year * 0.0491 if age_group_id == 14
	replace age_spec_5year = scaled_5year * 0.0709 if age_group_id == 15
	replace age_spec_5year = scaled_5year * 0.0964 if age_group_id == 16
	replace age_spec_5year = scaled_5year * 0.1336 if age_group_id == 17
	replace age_spec_5year = scaled_5year * 0.1423 if age_group_id == 18
	replace age_spec_5year = scaled_5year * 0.1477 if age_group_id == 19
	replace age_spec_5year = scaled_5year * 0.1413 if age_group_id == 20
	replace age_spec_5year = scaled_5year * 0.1487 if age_group_id == 21
	
	
	sort location_id sex year
	
	by location_id sex year: egen age_std = sum(age_spec_5year )
	
	keep if age_group_id == 1
	
	keep if year == 2004
	
	merge 1:1 acause year location_id sex using "FILEPATH/eurocare_5year_survival_formatted.dta"
	
	scatter eurocare_survival age_std , by(sex, title("`cancer' GBD vs. EUROCARE")  legend(off) note("age-standardized survival, by sex"))  || line eurocare_survival eurocare_survival ,  xtitle("GBD predicted survival") ytitle("EUROCARE relative survival") xscale(range(0 1)) yscale(range(0 1))
	graph export "${EUROCAREplots}//`cancer'_EUROCARE_GBD.png", replace
	

}	



////// for cancers with distribution 2: nasopharynx, cervical, brain, thyroid
set more off

global EUROCAREplots "FILEPATH"

local cancers "neo_nasopharynx neo_cervical neo_thyroid"

foreach cancer of local cancers {

	use "FILEPATH/`cancer'_predicted_survival.dta", clear

	gen age_spec_5year = .
	replace age_spec_5year = scaled_5year * 0.0113 if age_group_id == 8
	replace age_spec_5year = scaled_5year * 0.0201 if age_group_id == 9
	replace age_spec_5year = scaled_5year * 0.0374 if age_group_id == 10
	replace age_spec_5year = scaled_5year * 0.0575 if age_group_id == 11
	replace age_spec_5year = scaled_5year * 0.0754 if age_group_id == 12
	replace age_spec_5year = scaled_5year * 0.0782 if age_group_id == 13
	replace age_spec_5year = scaled_5year * 0.0828 if age_group_id == 14
	replace age_spec_5year = scaled_5year * 0.0872 if age_group_id == 15
	replace age_spec_5year = scaled_5year * 0.0990 if age_group_id == 16
	replace age_spec_5year = scaled_5year * 0.1110 if age_group_id == 17
	replace age_spec_5year = scaled_5year * 0.1100 if age_group_id == 18
	replace age_spec_5year = scaled_5year * 0.0900 if age_group_id == 19
	replace age_spec_5year = scaled_5year * 0.0725 if age_group_id == 20
	replace age_spec_5year = scaled_5year * 0.0675 if age_group_id == 21	
	sort location_id sex year
	
	by location_id sex year: egen age_std = sum(age_spec_5year )
	
	keep if age_group_id == 1
	
	keep if year == 2004
	
	merge 1:1 acause year location_id sex using "FILEPATH/eurocare_5year_survival_formatted.dta"
	
	scatter eurocare_survival age_std , by(sex, title("`cancer' GBD vs. EUROCARE")  legend(off) note("age-standardized survival, by sex"))  || line eurocare_survival eurocare_survival ,  xtitle("GBD predicted survival") ytitle("EUROCARE relative survival") xscale(range(0 1)) yscale(range(0 1))
	graph export "${EUROCAREplots}//`cancer'_EUROCARE_GBD.png", replace
	

}


////// for cancers with distribution 3: testicular, hodgkins, ALL
set more off

global EUROCAREplots "FILEPATH"

local cancers "neo_testicular neo_cervical neo_leukemia_ll_acute"

foreach cancer of local cancers {

	use "FILEPATH/`cancer'_predicted_survival.dta", clear

	gen age_spec_5year = .
	replace age_spec_5year = scaled_5year * 0.0694 if age_group_id == 8
	replace age_spec_5year = scaled_5year * 0.1118 if age_group_id == 9
	replace age_spec_5year = scaled_5year * 0.1297 if age_group_id == 10
	replace age_spec_5year = scaled_5year * 0.1163 if age_group_id == 11
	replace age_spec_5year = scaled_5year * 0.0963 if age_group_id == 12
	replace age_spec_5year = scaled_5year * 0.0765 if age_group_id == 13
	replace age_spec_5year = scaled_5year * 0.0532 if age_group_id == 14
	replace age_spec_5year = scaled_5year * 0.0468 if age_group_id == 15
	replace age_spec_5year = scaled_5year * 0.0522 if age_group_id == 16
	replace age_spec_5year = scaled_5year * 0.0478 if age_group_id == 17
	replace age_spec_5year = scaled_5year * 0.0507 if age_group_id == 18
	replace age_spec_5year = scaled_5year * 0.0493 if age_group_id == 19
	replace age_spec_5year = scaled_5year * 0.0518 if age_group_id == 20
	replace age_spec_5year = scaled_5year * 0.0482 if age_group_id == 21
	
	sort location_id sex year
	
	by location_id sex year: egen age_std = sum(age_spec_5year )
	
	keep if age_group_id == 1
	
	keep if year == 2004
	
	merge 1:1 acause year location_id sex using "FILEPATH/eurocare_5year_survival_formatted.dta"
	
	scatter eurocare_survival age_std , by(sex, title("`cancer' GBD vs. EUROCARE")  legend(off) note("age-standardized survival, by sex"))  || line eurocare_survival eurocare_survival ,  xtitle("GBD predicted survival") ytitle("EUROCARE relative survival") xscale(range(0 1)) yscale(range(0 1))
	graph export "${EUROCAREplots}//`cancer'_EUROCARE_GBD.png", replace
	

}

//








