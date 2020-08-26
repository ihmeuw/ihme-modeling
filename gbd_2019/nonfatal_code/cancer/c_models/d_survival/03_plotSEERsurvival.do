/////////////////////////////////////////////////////////
// Name of script: plotSEERsurvival.do
// Description: Generates plots of SEER survival data for diagnostics.
// Arguments: Requires formatted SEER data in appropriate format.
// Outputs: Cancer-specific plots of survival over time.
// Author(s): NAME
// Last updated: 2018-09-27	


////// diagnostic plots of survival data
set more off

global SEERdata "FILEPATH"
global SEERsurvPlots "FILEPATH"

local TimeRange "yrs_2000_2009 yrs_2001_2010 yrs_2005_2014 yrs_2000_2014"
local cancers "neo_bladder neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_lung neo_lymphoma neo_melanoma neo_meso neo_mouth neo_myeloma neo_nasopharynx neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_uterine"
	
	// also plotting several aggregate year ranges
	// for year_test2: (2020 ~ 2000-2009, at least 6 years) (2021 ~ 2001-2010, at least 5 years)
	// 				   (2022 ~ 2005-2014, at least 1 year) (2023 ~ 2000-2014, covers range of 15 years)
foreach cancer of local cancers {
	import delimited "${SEERdata}\\`cancer'_SEERsurv10yrs_formatted.txt"
	
	gen year_test = year_dx_aggregate
	replace year_test = . if year_dx_aggregate >= 16
	replace year_test = year_test + 2000
	
	gen year_test2 = year_test
	replace year_test2 = 2020 if year_dx_aggregate == 16	
	replace year_test2 = 2021 if year_dx_aggregate == 17 	
	replace year_test2 = 2022 if year_dx_aggregate == 19	
	replace year_test2 = 2023 if year_dx_aggregate == 18	

	gen age_group = ""
	replace age_group = "00-04" if age_gbd == 0
	replace age_group = "05-09" if age_gbd == 1
	replace age_group = "10-14" if age_gbd == 2
	replace age_group = "15-19" if age_gbd == 3
	replace age_group = "20-24" if age_gbd == 4
	replace age_group = "25-29" if age_gbd == 5
	replace age_group = "30-34" if age_gbd == 6
	replace age_group = "35-39" if age_gbd == 7
	replace age_group = "40-44" if age_gbd == 8
	replace age_group = "45-49" if age_gbd == 9
	replace age_group = "50-54" if age_gbd == 10
	replace age_group = "55-59" if age_gbd == 11
	replace age_group = "60-64" if age_gbd == 12
	replace age_group = "65-69" if age_gbd == 13
	replace age_group = "70-74" if age_gbd == 14
	replace age_group = "75-79" if age_gbd == 15
	replace age_group = "80-84" if age_gbd == 16
	replace age_group = "80+" if age_gbd == 17
	replace age_group = "85+" if age_gbd == 18
	
	foreach xyear of numlist 1 2 3 4 5 6 7 8 9 10 {
		scatter relative year_test2 if surv_year == `xyear' & sex==1 & n >= 25,  by(age_group, title("SEER `xyear'-year survival, `cancer'")) mcolor(blue) ytitle("Relative survival") ylabel(0(20)100) xtitle("Year of diagnosis (far right is for cumulative ranges)") || scatter relative year_test2 if surv_year == `xyear' & sex==1 & n < 25 & n >= 10,  by(age_group, legend(off) note("Blue: Male, Red: Female  ||   dot: >=25 cases, circle: 10-24 cases, x: <10 cases")) mcolor(blue) msymbol(Oh) || scatter relative year_test2 if surv_year == `xyear' & sex==1 & n < 10,  by(age_group) mcolor(blue) msymbol(x) || scatter relative year_test2 if surv_year == `xyear' & sex==2 & n >= 25,  by(age_group) mcolor(red) || scatter relative year_test2 if surv_year == `xyear' & sex==2 & n < 25 & n >=10,  by(age_group) mcolor(red) msymbol(Oh) || scatter relative year_test2 if surv_year == `xyear' & sex==2 & n < 10,  by(age_group) mcolor(red) msymbol(x)
		graph export "${SEERsurvPlots}\\`cancer'_SEER_`xyear'yr_survival.png", replace
	}

	foreach years of local TimeRange {
		scatter relative surv_year if year_range == "`years'" & sex==1 & n >= 25,  by(age_group, title("SEER survival for `years', `cancer'")) mcolor(blue) ytitle("Relative survival") ylabel(0(20)100) xtitle("Years of survival") || scatter relative surv_year if year_range == "`years'" & sex==1 & n < 25 & n >= 10,  by(age_group, legend(off) note("Blue: Male, Red: Female  ||   dot: >=25 cases, circle: 10-24 cases, x: <10 cases")) mcolor(blue) msymbol(Oh) || scatter relative surv_year if year_range == "`years'" & sex==1 & n < 10,  by(age_group) mcolor(blue) msymbol(x) || scatter relative surv_year if year_range == "`years'" & sex==2 & n >= 25,  by(age_group) mcolor(red) || scatter relative surv_year if year_range == "`years'" & sex==2 & n < 25 & n >=10,  by(age_group) mcolor(red) msymbol(Oh) || scatter relative surv_year if year_range == "`years'" & sex==2 & n < 10,  by(age_group) mcolor(red) msymbol(x)
		graph export "${SEERsurvPlots}\\`cancer'_SEER_curves_`years'.png", replace
	}

	clear
}

// end of script


