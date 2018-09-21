** ****************************************************
** Purpose: Final clean and standarization of data before uploading to the CoD database
** ******************************************************************************************************************
 // Prep Stata
	clear all
	set more off, perm
	if c(os) == "Unix" {
		set mem 10G
		set odbcmgr unixodbc
		global j "/home/j"
	}
	else if c(os) == "Windows" {
		set mem 800m
		global j "J:"
	}
	
// Source
	global source "`1'"

// Date
	global timestamp "`2'"
	
// Username
	global username "`3'"
	
//  Local for the directory where all the COD data are stored
	local in_dir "$j/WORK/03_cod/01_database/03_datasets"
	global in_dir "$j/WORK/03_cod/01_database/03_datasets"

// Local for the output directory
	local out_dir "$j/WORK/03_cod/01_database/02_programs/compile"

// Log output
	capture log close _all
	log using "`in_dir'//$source//logs/12_final_clean_${timestamp}", replace

// Prep the envelope (national only)
	do "$j/WORK/03_cod/01_database/02_programs/prep/code/env_wide.do"
	keep if location_id == .
	reshape long pop env, i(iso3 year sex) j(age)
	rename env mean_env
	rename pop mean_pop
	replace age = (age-6)*5 if age!=3 & age<90
	replace age = 1 if age==3
	tempfile pop
	save `pop', replace
	
// Get age sex restrictions
	use "$j/WORK/00_dimensions/03_causes/gbd2015_causes_all.dta", clear
	keep acause yld_only
	tempfile agesexrestrictions
	save `agesexrestrictions', replace
	
// Bring in data
	use "`in_dir'/$source/data/final/11_noise_reduced.dta", clear
	
// **********************************************************************************************************************************************
// APPLY NON-ZERO FLOOR OF 1 DEATH PER 10,000,000
	// Attach envelope
		drop if year < 1970
		merge m:1 iso3 year age sex using `pop', assert(2 3) keep(3) nogen
		gen double rate = (cf_final * mean_env) / mean_pop

	// Make death rates and calculate the cf if the rate were 2 MADs below the "global" median 
	// Every cause in the floor file is checked to ensure non-zero values in any non-restricted age-sex. Ensure something there for the cause, filling in zeroes where missing if the cause is present in the floor file 
		merge m:1 acause year age sex using "`out_dir'/data/inputs/nonzero_floor_mad.dta", keep(1 3) keepusing(floor) nogen
		egen min_floor = min(floor), by(acause)
		count if min_floor == . & cf_final > 0
		capture assert `r(N)' == 0
		if _rc {
			merge m:1 acause using "$j/WORK/00_dimensions/03_causes/gbd2015_causes_all.dta", assert(2 3) keep(3) keepusing(yld_only) nogen
			levelsof acause if min_floor == . & yld_only !=1,local(bad_causes) c
			if "`bad_causes'" != "" {
				di "There are non-restricted cause-age-sex observations that are missing or zero (they will be kept in memory)"
				keep if min_floor == . & yld_only !=1
				BREAK
			}
			drop yld_only
		}
		replace floor = min_floor if floor == .
		replace floor = 0 if floor == .
		gen double cf_replace = (floor * mean_pop) / mean_env
	// Replace the CF with the rate-adjusted CF if the rate is less than the floor and greater than zero
		replace cf_final = cf_replace if rate < floor & rate > 0
	// Don't want data with 0 sample_size
		drop if sample_size == 0
		
	// Tidy up
		drop mean_pop rate* cf_replace mean_env *floor*

// **********************************************************************************************************************************************
//  AGGREGATE SUBNATIONAL DATASETS TO NATIONAL AGGREGATES - Brazil, China, UK, Japan, Mexico, Sweden, Saudi Arabia, USA, South Africa
	if inlist("$source", "UK_1981_2000", "UK_2001_2011", "Brazil_SIM_ICD9", "Brazil_SIM_ICD10", "Japan_by_prefecture_ICD10", "Japan_by_prefecture_ICD9") | inlist("$source", "US_NCHS_counties_ICD9", "US_NCHS_counties_ICD10", "South_Africa_by_province", "China_1991_2002", "China_2004_2012") | inlist("$source", "ICD9_detail", "ICD10", "Sweden_ICD9", "Sweden_ICD10", "ICD8_detail","Saudi_Arabia_96_2012") | inlist("$source", "India_SCD_states_rural", "India_MCCD_states_ICD10", "India_MCCD_states_ICD9", "India_SRS_states_report") | "$source" == "Other_Maternal" | "$source" == "Mexico_BIRMM" {
		do "`out_dir'/code/subnational_aggregation.do" "clean"
	}

// **********************************************************************************************************************************************
//  Cancer Registry Correction - Drop data that is fractional when scaled to the national envelope
	if "$source" == "Cancer_Registry" {
		gen test_deaths = cf_final * pop 
		drop if test_deaths < 1
		drop test_deaths
	}
	
// **********************************************************************************************************************************************
// AGGREGATE TO ALL AGES AND CALCULATE AGE-STANDARDIZED RATE BEFORE SAVING
	// perform function
		do "`out_dir'/code/all_ages_age_stand.do"
		
// **********************************************************************************************************************************************
//  SAVE FINAL DATABASE AND UPLOAD
	// Prep Final Variables
	// Age
		gen double new_age = age
		drop age
		rename new_age age
		replace age = 0 if age == 91
		replace age = .01 if age == 93
		replace age = .1 if age == 94

	// ICD-version
		rename list icd_vers
		
	// Source_type
		replace source_type = "Verbal Autopsy" if regexm(source_type, "VA")
		replace source_type = "Vital Registration" if regexm(source_type, "VR")
		replace source_type = "Police Records" if regexm(source_type, "Police")
		replace source_type = "Hospital" if regexm(source_type, "Hospital")
		replace source_type = "Survey/Census" if source_type == "Survey" | source_type == "Census" | source_type == "Self Reported CoD"
		replace source_type = "Sibling History" if source_type == "Sibling history, survey" | source_type == "Sibling history"
		replace source_type = "Cancer Registry" if source_type == "IBMC"
		replace source_type = "Burial/Mortuary" if source_type == "Burial" | source_type == "Mortuary"
	// National type id
		cap rename national representative_id
		replace representative_id = 2 if representative_id == 0
	// Urbanicity type id
		gen urbanicity_type_id = 0
	// CF
		foreach var of varlist cf* {
		replace `var' = 1 if `var' > 1 & `var' != .
		replace `var' = 0 if `var' == . | `var' < 0
	}
	preserve
	
	// load iso3 to location_id map
		odbc load, exec("SELECT location_id, ihme_loc_id AS iso3 FROM shared.location_hierarchy_history WHERE location_set_version_id = 34 and location_type in('admin0','nonsovereign') ORDER BY sort_order") strConnection clear
		compress
		tempfile country_ids
		save `country_ids', replace

	// load age_ids
		odbc load, exec("SELECT age_group_id, age_group_name_short FROM shared.age_group where (age_group_id>=2 and age_group_id<=22) or age_group_id=27") strConnection clear
		replace age_group_name = "0" if age_group_name == "EN"
		replace age_group_name = "0.01" if age_group_name == "LN"
		replace age_group_name = "0.1" if age_group_name == "PN"
		** all ages
		replace age_group_name = "99" if age_group_id==22
		** age standardized
		replace age_group_name = "98" if age_group_id==27
		destring age_group_name, replace
		rename age_group_name age
		tempfile ages
		save `ages', replace
		
	// load data types
		odbc load, exec("select data_type_id, data_type_full as source_type from cod.data_type") strConnection clear
		tempfile data_types
		save `data_types', replace

	// load causes
		use acause cause_id using "$j/WORK/00_dimensions/03_causes/gbd2015_causes_all.dta", clear
		tempfile causes
		save `causes', replace

	// merge  data
		restore
		// rename ids
		rename sex sex_id
		rename year year_id
		// merge cause_id
		merge m:1 acause using `causes', assert(using matched) keep(3) nogen
		// merge type_id (data_type)
		merge m:1 source_type using `data_types', assert(using matched) keep(1 3) nogen keepusing(data_type_id)
		// fill in all location_ids
		rename location_id subnational_id
		merge m:1 iso3 using `country_ids', assert(using matched) keep(1 3) nogen
		replace location_id = subnational_id if subnational_id != .
		drop subnational_id
		// merge age_group_id
		merge m:1 age using `ages', assert(using matched) keep(3) nogen
		rename NID nid
		rename subdiv site
		
	// save before upload

		compress
		save "`in_dir'/$source//data/final/12_cleaned.dta", replace
		save "`in_dir'/$source//data/final/_archive/12_cleaned_$timestamp.dta", replace
		foreach var in predicted_cf predicted_var std_err_data variance_data variance deaths_final smooth cf_pred cf_post std_error se_pred var_post {
			capture drop `var'
		}
		