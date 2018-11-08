** **************************************************************************
** CONFIGURATION
** **************************************************************************
	** ****************************************************************
	** Prepare STATA for use
	**
	** This section sets the application perferences.  The local applications
	**	preferences include memory allocation, variables limits, color scheme,
	**	defining the drive (data), and setting a local for the date.
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
					global prefix "/FILEPATH/j"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "J:"
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
				local data_name "NOR_NIPH_08_12"
			// Original data folder
				local input_folder "$prefix{FILEPATH}/"
			// Log folder
				local log_folder "$prefix{FILEPATH}/"
				capture mkdir "`log_folder'"
			// Output folder
				local output_folder "$prefix{FILEPATH}/"
				local archive_folder "`output_folder'/"
				capture mkdir "`output_folder'"
				capture mkdir "`archive_folder'"


		** ****************************************************************
		** CREATE LOG
		** ****************************************************************
			capture log close
			log using "`log_folder'/00_format_`today'.log", replace


		// IHME written mata functions which speed up collapse and egen
			do "$prefix{FILEPATH}/fastcollapse.ado" 

** **************************************************************************
** RUN FORMAT PROGRAGM
** **************************************************************************
	// GET DATA
		use "`input_folder'/03 Alle_opphold NPR_2008_2012_4tegns_translated.dta", clear

	// ENSURE ALL VARIABLES ARE PRESENT
		// source (string): source name
			gen source = "`data_name'"
		// NID (numeric)
			gen NID = .
			replace NID = 149500 if year == 2008
			replace NID = 149501 if year == 2009
			replace NID = 149502 if year == 2010
			replace NID = 149503 if year == 2011
			replace NID = 149504 if year == 2012
		// iso3 (string)
			gen iso3 = "NOR"
		// subdiv (string)
			gen subdiv = ""
		// location_id (numeric)
			gen location_id = .
		// national (numeric): 0 = no, 1 = yes
			gen national = 1
		// year (numeric)
			** already present
		// age (numeric)
			** already present
		// frmat (numeric): find the WHO format here "FILEPATH\Age formats documentation.xlsx"
			gen frmat = .
		// im_frmat (numeric): from the same file as above
			gen im_frmat = .
		// sex (numeric): 1=male 2=female 9=missing
			** already present
		// platform (string): "Inpatient", "Outpatient"
			gen platform = "Inpatient"
			replace platform = "Outpatient" if polyclinic == 1
		// patient_id (string)
			gen patient_id = ""
		// icd_vers (string): ICD version - "ICD10", "ICD9_detail"
			gen icd_vers = "ICD10"
		// dx_* (string): diagnoses
			rename cause_code dx_1
		// ecode_* (string): variable if E codes are specifically mentioned
			gen ecode_1 = ""
		// Inpatient variables
			// admissions (numeric)
				gen metric_discharges = counts if inpatient == 1
				gen metric_day_cases = counts if day_case == 1
		// Outpatient variables
			// visits (numeric)
				gen metric_visits = counts if polyclinic == 1

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

	// COLLAPSE DATA
		collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_* ecode_*) fast

** **************************************************************************
** RUN SELECT PRIMARY PROGRAGM
** **************************************************************************
		gen cause_primary = dx_1
		// Prioritize External Cause of Injury & Service Codes over Nature of Injury
			// Diagnosis codes
				local dx_count = 0
				foreach var of varlist dx_* {
					local dx_count = `dx_count' + 1
				}
				local dx_count = `dx_count'
				// Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes
					forvalues dx = `dx_count'(-1)1 {
						display "Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes in dx_`dx'"
						replace cause_primary = dx_`dx' if (inlist(substr(dx_`dx',1,1),"V","W","X","Y") & icd_vers == "ICD10") | (inlist(substr(dx_`dx',1,1),"E") & icd_vers == "ICD9_detail")
					}
			// External cause set
				local ecode_count = 0
				foreach var of varlist ecode_* {
					local ecode_count = `ecode_count' + 1
				}
				local ecode_count = `ecode_count'
				// Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes
					forvalues dx = `ecode_count'(-1)1 {
						display "Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes in ecode_`dx'"
						replace cause_primary = ecode_`dx' if (inlist(substr(ecode_`dx',1,1),"V","W","X","Y") & icd_vers == "ICD10") | (inlist(substr(ecode_`dx',1,1),"E") & icd_vers == "ICD9_detail")
					}	
		// Reshape long
			drop dx_1
			rename cause_primary dx_1			. gen long id = _n
			reshape long dx_ ecode_, i(id) j(dx_ecode_id)
			replace dx_ecode_id = 2 if dx_ecode_id > 2
			drop if missing(dx_)
			//drop id

		// Collapse
			keep iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_ecode_id metric_* dx_ ecode_

** **************************************************************************
** RUN EPI COMPILE HOSPITAL CLUSTER PROGRAGM
** **************************************************************************
	// STANDARDIZE PLATFORMS
		replace platform = "Outpatient" if platform == "Emergency"

		replace platform = "1" if platform == "Inpatient" | platform == "Inpatient 1"
		drop if platform == "Inpatient 2"
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
		
	// STANDARDIZE AGES
		drop if age == .
		replace age = 95 if age > 95
		replace age = 0 if age < 1

		rename age age_start
		gen age_end = age_start + 4
		replace age_end = 1 if age_start == 0
		replace age_end = 99 if age_start == 95
		replace age_end = 4 if age_start == 1

	// LOCATION_ID
		rename location_id location_id_orig
		merge m:1 iso3 using "$prefix{FILEPATH}/location_ids.dta"
			assert _m != 1
			drop if _m == 2
			drop _m
		replace location_id = location_id_orig if location_id_orig != .
		drop location_id_orig
		
	// COLLAPSE CASES
		collapse (sum) cases, by(iso3 subdiv location_id source national year age_* sex platform icd_vers dx_ dx_ecode_id NID) fast

	// modify columns to fit our structure
		rename dx_ cause_code
		rename dx_ecode_id diagnosis_id

	// SAVE
		compress
		save "`output_folder'/formatted_NOR_NIPH_08_12.dta", replace
		save "`archive_folder'/formatted_NOR_NIPH_08_12_`today'.dta", replace

	capture log close





// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
