 *************************************************************************
** Purpose: create individual datasheet for each gbd_cause item generated from FAO
**  Note: This step brings in the datasheet where standard error has already been imputed
**
** **************************************************************************
** RUNTIME CONFIGURATION
** **************************************************************************
// Set preferences for STATA
	// Clear memory and set memory and variable limits
		clear all
	// Set to run all selected code without pausing
		set more off
	// Remove previous restores
		capture restore, not
		if c(os) == "Unix" {
			global prefix "FILEPATH"
			set odbcmgr unixodbc
		}
		else if c(os) == "Windows" {
			global prefix "FILEPATH"
		}

**call in arguments from parent script
local version "GBD2021"
local output_f "FILEPATH"   
capture confirm file `output_f'
if _rc mkdir `output_f'

**creating a skeleton file that will be used to create a "complete square" dataset: 

preserve
	import delimited "FILEPATH/calcium_g_unadj.csv", clear

		keep location_id year_id 
		duplicates drop
			tempfile skeleton
			save `skeleton', replace
restore

*Bring in the energy unadjusted datasheet
use "FILEPATH/FAO_sales_raw_data_variance_unadj_GBD2021.dta", clear
rename ihme_risk risk 
levelsof gbd_cause, local(causes)
foreach cause of local causes {
	preserve
	keep if gbd_cause == "`cause'"
	drop risk total_calories
	gen sex_id = 3
	rename year year_id
	gen age_group_id = 22
	gen sample_size = .
	//gen standard_deviation = .
	//gen placeholder = 1
	gen is_outlier = 0
	gen measure = "continuous"
	tempfile saved_`cause'
	save `saved_`cause'', replace
	use `skeleton', clear
	merge 1:m location_id year_id using `saved_`cause'', nogen
	gen me_name = gbd_cause+"_g_unadj"
	rename grams_daily_unadj data
	replace gbd_cause = "`cause'"
	replace gbd_cause = "energy_sua" if gbd_cause == "energy_kcal_unadj"
	replace gbd_cause = "`cause'_mg" if gbd_cause == "cholesterol" | gbd_cause == "salt" | gbd_cause == "iron" | gbd_cause == "magnesium" | gbd_cause == "phosphorus" | gbd_cause == "potassium"
	replace gbd_cause = "`cause'_kcal" if gbd_cause == "energy" | gbd_cause == "energy_sua"
	replace gbd_cause = "`cause'_%" if gbd_cause == "transfat" | gbd_cause == "saturated_fats" | gbd_cause == "pufa" | gbd_cause == "mufa" | gbd_cause == "carbohydrates"  | gbd_cause == "protein" | gbd_cause == "starch" | gbd_cause == "fats" 
	replace gbd_cause = "`cause'_ug" if gbd_cause == "vit_a_rae" | gbd_cause == "vit_a_retinol" | gbd_cause == "selenium" | gbd_cause == "folates" | gbd_cause == "folic_acid"
	replace me_name = gbd_cause+"_g_unadj"
	replace me_name = gbd_cause+"_unadj" if gbd_cause == "`cause'_mg" | gbd_cause == "`cause'_kcal" | gbd_cause == "`cause'_%" | gbd_cause == "`cause'_ug" | gbd_cause == "vit_a_iu"
	
	if me_name == "energy_kcal_unadj" {
		replace data = . if data < 100
	}
	replace data = . if ihme_loc_id == "GRD" & year_id == 2004 & gbd_cause == "cholesterol_mg"

	drop if variance == . & data != .
	drop if data == .

	**save each individual dataset
	if me_name == "`cause'_mg_unadj" {
		export delimited "`output_f'/`cause'_mg_unadj.csv", replace
		
	}
	if me_name == "`cause'_kcal_unadj" {
		if "`cause'" == "energy"  export delimited "`output_f'/`cause'_kcal_unadj.csv", replace
		if "`cause'" == "energy_kcal_unadj"  export delimited "`output_f'/energy_sua_kcal_unadj.csv", replace
	}
	if me_name == "`cause'_%_unadj" {
		export delimited "`output_f'/`cause'_%_unadj.csv", replace
	}
	if me_name == "`cause'_%" {
		export delimited "`output_f'/`cause'_%.csv", replace
	}
	if me_name == "`cause'_ug_unadj" {
		export delimited "`output_f'/`cause'_ug_unadj.csv", replace
	}
	if me_name == "vit_a_iu_unadj" {
		export delimited "`output_f'/vit_a_iu_unadj.csv", replace
	}
	if me_name == "`cause'_g_unadj" {
		export delimited "`output_f'/`cause'_g_unadj.csv", replace
	}
	restore
}




