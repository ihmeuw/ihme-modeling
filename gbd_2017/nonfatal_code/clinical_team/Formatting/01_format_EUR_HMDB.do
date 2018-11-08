

** **************************************************************************
** ANALYSIS CONFIGURATION
** **************************************************************************

	** ****************************************************************
	** Prepare STATA for use
	**
	** This section sets the application perferences and defines the local variables.
	** 	The local applications preferences include memory allocation, variables
	**	limits, color scheme, and defining the J drive (data).
	**
	** ****************************************************************
		// Set application preferences
			// Clear memory and set memory and variable limits
				clear all
				set mem 10G
				set maxvar 32000

			// Set to run all selected code without pausing
				set more off

			// Set graph output color scheme
				set scheme s1color

			// Define J drive (data) for cluster (UNIX) and Windows (Windows)
				if c(os) == "Unix" {
					global prefix "DRIVE"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "DRIVE"
				}

			// Get date
				local today = date(c(current_date), "DMY")
				local year = year(`today')
				local month = string(month(`today'),"%02.0f")
				local day = string(day(`today'),"%02.0f")
				local today = "`year'_`month'_`day'"


		** ****************************************************************
		** SET LOCALS
		**
		** Set data_name local and create associated folder structure for
		**	formatting prep.
		**
		** ****************************************************************
			// Data Source Name
				local data_name "EUR_HMDB"
			// Original data folder
				local input_folder "$prefix{FILEPATH}/"
			// Log folder
				local log_folder "$prefix{FILEPATH}/"
				capture mkdir "`log_folder'"
			// Output folder
				local output_folder "$prefix{FILEPATH}/"
				local archive_folder "`output_folder'/_archive"
				capture mkdir "`output_folder'"
				capture mkdir "`archive_folder'"


		** ****************************************************************
		** CREATE LOG
		** ****************************************************************
			capture log close
			log using "`log_folder'/00_format_`today'.log", replace

	** ****************************************************************
	** Prepare STATA for use
	** ****************************************************************
		use iso3 iso2 countryname_ihme using "$prefix{FILEPATH}/IHME_COUNTRYCODES.DTA", clear
		rename countryname_ihme country_name
		// Drop South Africa subregions
			drop if inlist(iso3,"ZEC", "ZFS", "ZGA", "ZKN", "ZLI", "ZMP", "ZNC", "ZNW", "ZWC")
		duplicates drop
		drop if iso2 == ""
		tempfile country_codes
		save `country_codes', replace

** **************************************************************************
** RUN PROGRAGM
** **************************************************************************
	// Get converted file
		use "`input_folder'/EUR_HMDB_90_14.DTA", clear
	// Make all field names lowercase
		foreach var of varlist * {
			local new_name = lower(trim("`var'"))
			capture rename `var' `new_name'
		}
	// Rename identifying variables
		rename country iso2
		rename yid year
		rename coding icd_vers
	// Convert to ISO3
		replace iso2 = "GB" if iso2 == "UK"
		merge m:1 iso2 using `country_codes', keep(1 3)
	// Sex
		// keep if gender == 1 | gender == 2
		rename gender sex
	// Collapse down to unique values
		collapse (sum) ds* bd* dc*, by(iso3 country_name agegroup year sex icd_vers code) fast
	// Assign Age Groups
	** **********************************************************************
	** NOTE: This file contains two age formats, 1, 2, and 4, as defined by the WHO.  This section reshapes the data so that the age formats are standardized.
	** **********************************************************************
		// Reshape
			reshape long ds bd dc, i(iso3 country_name agegroup year sex icd_vers code) j(age_wide)
		// Drop if all values are 0
			drop if ds == 0 & bd == 0 & dc == 0
		// Drop age_wide 0 (this is an all ages variable)
			drop if age_wide == 0
		// Drop "Unknown" age groups
			drop if agegroup == 1 & age_wide == 22
			drop if agegroup == 4 & age_wide == 20
		// Drop "Total" code
			drop if code == "TOT"
		// Create ages
			gen age = .
			// Age Group 1
				replace age = 0 if agegroup == 1 & age_wide == 1
				replace age = 1 if agegroup == 1 & age_wide == 2
				replace age = 5 if agegroup == 1 & age_wide == 3
				replace age = 10 if agegroup == 1 & age_wide == 4
				replace age = 15 if agegroup == 1 & age_wide == 5
				replace age = 20 if agegroup == 1 & age_wide == 6
				replace age = 25 if agegroup == 1 & age_wide == 7
				replace age = 30 if agegroup == 1 & age_wide == 8
				replace age = 35 if agegroup == 1 & age_wide == 9
				replace age = 40 if agegroup == 1 & age_wide == 10
				replace age = 45 if agegroup == 1 & age_wide == 11
				replace age = 50 if agegroup == 1 & age_wide == 12
				replace age = 55 if agegroup == 1 & age_wide == 13
				replace age = 60 if agegroup == 1 & age_wide == 14
				replace age = 65 if agegroup == 1 & age_wide == 15
				replace age = 70 if agegroup == 1 & age_wide == 16
				replace age = 75 if agegroup == 1 & age_wide == 17
				replace age = 80 if agegroup == 1 & age_wide == 18
				replace age = 85 if agegroup == 1 & age_wide == 19
				replace age = 90 if agegroup == 1 & age_wide == 20
				replace age = 95 if agegroup == 1 & age_wide == 21
			// Age Group 2
				replace age = 0 if agegroup == 2 & age_wide == 1
				replace age = 15 if agegroup == 2 & age_wide == 2
				replace age = 65 if agegroup == 2 & age_wide == 3
			// Age Group 4
				replace age = 0 if agegroup == 4 & age_wide == 1
				replace age = 1 if agegroup == 4 & age_wide == 2
				replace age = 5 if agegroup == 4 & age_wide == 3
				replace age = 10 if agegroup == 4 & age_wide == 4
				replace age = 15 if agegroup == 4 & age_wide == 5
				replace age = 20 if agegroup == 4 & age_wide == 6
				replace age = 25 if agegroup == 4 & age_wide == 7
				replace age = 30 if agegroup == 4 & age_wide == 8
				replace age = 35 if agegroup == 4 & age_wide == 9
				replace age = 40 if agegroup == 4 & age_wide == 10
				replace age = 45 if agegroup == 4 & age_wide == 11
				replace age = 50 if agegroup == 4 & age_wide == 12
				replace age = 55 if agegroup == 4 & age_wide == 13
				replace age = 60 if agegroup == 4 & age_wide == 14
				replace age = 65 if agegroup == 4 & age_wide == 15
				replace age = 70 if agegroup == 4 & age_wide == 16
				replace age = 75 if agegroup == 4 & age_wide == 17
				replace age = 80 if agegroup == 4 & age_wide == 18
				replace age = 85 if agegroup == 4 & age_wide == 19
				replace age = 90 if agegroup == 4 & age_wide == 20
				replace age = 95 if agegroup == 4 & age_wide == 21

			// Check for missing groups
				assert age != .

			// Drop unneeded age variables
				drop age_wide agegroup
	// Rename variables of interest
		rename ds discharge
		rename bd bed_days
		rename dc day_cases
		rename code dx
	// Collapse down last time
		collapse (sum) discharge bed_days day_cases, by(iso3 year sex age icd_vers dx) fast
	// Reorder & Reformat
		order iso3 year sex age icd_vers dx discharge bed_days day_cases
		sort iso3 year sex age icd_vers dx discharge bed_days day_cases

// Now begin formatting
	// DROP DATA THAT CONFLICTS WITH OTHER SOURCES
		drop if inlist(iso3, "AUT", "NOR", "SWE", "GBR")
		
	// DROP DATA NOT IN ICD
		drop if icd_vers == "HMT"

	// ENSURE ALL VARIABLES ARE PRESENT
		// source (string): source name
			gen source = "`data_name'"
		// NID (numeric)
			gen NID = 3822
		// iso3 (string)
			** already defined
		// subdiv (string)
			gen subdiv = ""
		// location_id (numeric)
			gen location_id = .
		// national (numeric): 0 = no, 1 = yes
			gen national = 1
		// year (numeric)
			** already defined
		// age (numeric)
			** already defined
			assert age != .
		// frmat (numeric): find the WHO format here "{FILEPATH}/Age formats documentation.xlsx"
			gen frmat = 2
			replace frmat = 112 if iso3 == "ISR"
		// im_frmat (numeric): from the same file as above
			gen im_frmat = 8
		// sex (numeric): 1=male 2=female 9=missing
			replace sex = 3 if sex == 0
		// platform (string): "Inpatient", "Outpatient"
			gen platform = "Inpatient"
		// patient_id (string)
			gen patient_id = ""
		// icd_vers (string): ICD version - "ICD10", "ICD9_detail"
			replace icd_vers = "ICD10" if substr(icd_vers,1,6) == "103"
			replace icd_vers = "ICD9_detail" if substr(icd_vers,1,5) == "093"
		// dx_* (string): diagnoses
			rename dx dx_1
		// ecode_* (string): variable if E codes are specifically mentioned
			gen ecode_1 = ""
		// Inpatient variables
			// admissions (numeric)
				rename discharge metric_discharges
			// bed_days (numeric)
				rename bed_days metric_bed_days
			// day_causes (numeric)
				rename day_cases metric_day_cases
	// Collapse
		collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_* ecode_*) fast

	// Clean aggregated tabulation
	// Make a map for digit ICD codes
		gen parent_icd9 = substr(dx_1,1,3) if icd_vers == "ICD9_detail"
		replace parent_icd9 = substr(dx_1,1,4) if substr(dx_1,1,1) == "E" & icd_vers == "ICD9_detail"
		gen parent_icd10 = substr(dx_1,1,3) if icd_vers == "ICD10"
	// Flag parent diagnoses and create families
		tempfile master
		save `master', replace
		// Families for ICD9
			keep if icd_vers == "ICD9_detail"
			gen parent_code9 = 1
			collapse parent_code9, by(parent_icd9 icd_vers)
			gen dx_1 = parent_icd9
			tempfile map_icd9
			save `map_icd9'
		// Families for ICD10
			use `master', clear
			keep if icd_vers == "ICD10"
			gen parent_code10 = 1
			collapse parent_code10, by(parent_icd10 icd_vers)
			gen dx_1 = parent_icd10
			tempfile map_icd10
			save `map_icd10'
		// Merge on flag
			use `master', clear
			keep if icd_vers == "ICD9_detail" | icd_vers == "ICD10"
			merge m:1 dx_1 icd_ver using `map_icd9', nogen
			merge m:1 dx_1 icd_ver using `map_icd10', nogen
			gen parent_code = 0
			replace parent_code = 1 if parent_code9 == 1
			replace parent_code = 1 if parent_code10 == 1
	// Switch metrics with parent flag
		foreach metric in metric_discharges metric_bed_days metric_day_cases {
			replace `metric' = `metric' * -1 if parent_code == 1
		}
	// Collapse families
			collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers parent_icd9 parent_icd10)
	// Rename variables for merge
		// Primary diagnosis
			gen dx_1 = parent_icd9
			replace dx_1 = parent_icd10 if dx_1 == ""
		// Metric variables
		foreach metric in metric_discharges metric_bed_days metric_day_cases {
			rename `metric' difference_`metric'
		}
	// Manual Correction
		drop if iso3 == "BEL" & substr(dx_1,1,1) == "E" & icd_vers == "ICD9_detail"
	// Save
		tempfile correction
		save `correction'
	// Apply correction
		use `master', clear
		merge 1:1 iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_1 using `correction', nogen keepusing(difference_*)
		foreach metric in metric_discharges metric_bed_days metric_day_cases {
			gen `metric'_original = `metric'
			gen is_aggregated_`metric' = 1
			replace is_aggregated_`metric' = 0 if `metric' == difference_`metric' * -1
			replace is_aggregated_`metric' = 0 if difference_`metric' == .
			replace `metric' = difference_`metric' * -1 if is_aggregated_`metric' == 1
		}
		drop if metric_discharges == 0 & metric_bed_days == 0 & metric_day_cases == 0

	// VARIABLE CHECK
		// If any of the variables in our template are missing, create them now (even if they are empty)
		// All of the following variables should be present
			#delimit;
			order
			iso3 subdiv location_id national
			source NID
			year
			age frmat im_frmat
			sex platform patient_id
			icd_vers dx_* ecode_*
			metric_*;
		// Drop any variables not in our template of variables to keep
			keep
			iso3 subdiv location_id national
			source NID
			year
			age frmat im_frmat
			sex platform patient_id
			icd_vers dx_* ecode_*
			metric_*;
			#delimit cr

	drop if inlist(iso3, "AUT", "SWE", "NOR", "GBR")


// STANDARDIZE PLATFORMS
	replace platform = "Outpatient" if platform == "Emergency"
	replace platform = "1" if platform == "Inpatient" | platform == "Inpatient 1"
	replace platform = "2" if platform == "Outpatient"
	destring platform, replace
	assert platform != .

// STANDARDIZE METRICS
	gen cases = .
		capture confirm var metric_discharges
		if !_rc {
			replace cases = metric_discharges if platform == 1
		}
		capture confirm var metric_discharges_weighted
		if !_rc {
			replace cases = metric_discharges_weighted if platform == 1
		}
		capture confirm var metric_visits
		if !_rc {
			replace cases = metric_visits if platform == 2
		}
		capture confirm var metric_visits_weighted
		if !_rc {
			replace cases = metric_visits_weighted if platform == 2
		}
	gen deaths = .
		capture confirm var metric_deaths
		if !_rc {
			replace deaths = metric_deaths
		}
		capture confirm var metric_deaths_weighted
		if !_rc {
			replace deaths = metric_deaths_weighted
		}

// STANDARDIZE AGES
	drop if age == .
	replace age = 95 if age > 95
	replace age = 0 if age < 1
	rename age age_start
	gen age_end = age_start + 4
	replace age_end = 1 if age_start == 0
	replace age_end = 99 if age_start == 95
	replace age_end = 4 if age_start == 1
	replace age_end = 14 if age_start == 0 & frmat == 112
	replace age_end = 64 if age_start == 15 & frmat == 112
	replace age_end = 99 if age_start == 65 & frmat == 112

// LOCATION_ID
	rename location_id location_id_orig
	merge m:1 iso3 using "$prefix{FILEPATH}/location_ids.dta"
		assert _m != 1
		drop if _m == 2
		drop _m
	replace location_id = location_id_orig if location_id_orig != .
	drop location_id_orig

	// DO FINAL COLLAPSE ON DATA
	collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age_start age_end frmat im_frmat sex platform patient_id icd_vers dx_* ecode_*) fast

	// modify columns to fit our structure
		drop ecode_1 frmat metric_day_cases_original im_frmat patient_id metric_discharges_original metric_bed_days_original
		rename dx_1 cause_code
		rename metric_discharges cases
		gen diagnosis_id = 1

	// SAVE
	compress
	save "`output_folder'/formatted_EUR_HMDB.dta", replace
	save "`archive_folder'/formatted_EUR_HDMB_`today'.dta", replace

	capture log close

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
