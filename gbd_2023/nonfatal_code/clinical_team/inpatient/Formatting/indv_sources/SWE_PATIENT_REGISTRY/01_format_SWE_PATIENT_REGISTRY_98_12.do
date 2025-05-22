// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author: 		USERNAME
// Date: 			October 24, 2014
// Modified:		--
// Project:		GBD
// Purpose:		Template for formatting DATA_SOURCE_NAME for the combined hospital database


** **************************************************************************
** CONFIGURATION
** **************************************************************************
	** ****************************************************************
	** Prepare STATA for use
	**
	** This section sets the application perferences.  The local applications
	**	preferences include memory allocation, variables limits, color scheme,
	**	defining the J drive (data), and setting a local for the date.
	**
	** ****************************************************************
		// Set application preferences
			// Clear memory and set memory and variable limits
				clear all
				set mem 10G


			// Set to run all selected code without pausing
				set more off

			// Set graph output color scheme
				set scheme s1color


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
				local data_name "SWE_PATIENT_REGISTRY_98_12"
			// Original data folder
				local input_folder "FILEPATH"
			// Log folder
				local log_folder "FILEPATH"
				capture mkdir "`log_folder'"
			// Output folder
				local output_folder "FILEPATH"
				local archive_folder "FILEPATH"
				capture mkdir "`output_folder'"
				capture mkdir "`archive_folder'"


		** ****************************************************************
		** CREATE LOG
		** ****************************************************************
			capture log close
			log using "`log_folder'/00_format_`today'.log", replace

	// IHME written mata functions which speed up collapse and egen
		do "FILEPATH"

** **************************************************************************
** RUN FORMATTING
** **************************************************************************
	// GET DATA
		// Inpatient data
			use "FILEPATH", clear
			gen platform = "Inpatient"
			rename vtf metric_discharges
		// Outpatient data
			append using "FILEPATH"
			replace platform = "Outpatient" if platform == ""
			rename vtf metric_visits

	// Remove labels
		label drop _all
		quietly ds
		foreach var in `r(varlist)' {
			label var `var' ""
		}

	// ENSURE ALL VARIABLES ARE PRESENT
		// source (string): source name
			gen source = "`data_name'"
		// iso3 (string)
			gen iso3 = "SWE"
		// subdiv (string)
			gen subdiv = ""
		// location_id (numeric)
			gen location_id = 93
		// national (numeric): 0 = no, 1 = yes
			gen national = 1
		// year (numeric)
			rename AR year
		// NID (numeric)
			gen NID = .
			replace NID = 121334 if year == 1998
			replace NID = 121405 if year == 1999
			replace NID = 121407 if year == 2000
			replace NID = 121408 if year == 2001
			replace NID = 121415 if year == 2002
			replace NID = 121416 if year == 2003
			replace NID = 121417 if year == 2004
			replace NID = 121418 if year == 2005
			replace NID = 121419 if year == 2006
			replace NID = 121420 if year == 2007
			replace NID = 121421 if year == 2008
			replace NID = 121422 if year == 2009
			replace NID = 121423 if year == 2010
			replace NID = 121424 if year == 2011
			replace NID = 121425 if year == 2012
		// age (numeric)
			replace age = "" if age == "-" | age == "." | age == "-1" | age == "36" | age == "37"
			rename age age_string
			gen age = substr(age_string,1,2)
			replace age = "" if substr(age,1,1) == "d"
			destring age, replace
			replace age = 0 if age_string == "d0-6"
			replace age = .01918 if age_string == "d7--27"
			replace age = .07671 if age_string == "d28-36" | age_string == "d28-364"
		// frmat (numeric): find the WHO format here "FILEPATH"
			gen frmat = 2
		// im_frmat (numeric): from the same file as above
			gen im_frmat = 2
		// sex (numeric): 1=male 2=female 9=missing
			rename KON sex
			replace sex = "" if sex == "." | sex == "0"
			destring sex, replace
			replace sex = 9 if sex == .
		// platform (string): "Inpatient", "Outpatient"
			** already present
		// patient_id (string)
			gen patient_id = ""
		// icd_vers (string): ICD version - "ICD10", "ICD9_detail"
			gen icd_vers = "ICD10"
		// dx_* (string): diagnoses
			rename HDIA dx_1
		// ecode_* (string): variable if E codes are specifically mentioned
			gen ecode_1 = ""
		// Metrics
			// discharges (numeric)
				** already present
		// Outpatient variables
			// visits (numeric)
				** already present

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
** RUN EPI COMPILE CLUSTER
** **************************************************************************

	gen dx_ecode_id = 1
	rename dx_1 dx_mapped_


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
		// oldest age in this source is 80!
		replace age = 80 if age > 80

		rename age age_start
		gen age_end = age_start + 4
		//replace age_end = 1 if age_start == 0
		replace age_end = 124 if age_start == 80
		replace age_end = 4 if age_start == 1
		// neonates
		replace age_end = .01918 if age_start == 0
		replace age_end = .07671 if age_start > 0.019 & age_start < 0.02
		replace age_end = 1 if age_start > 0.076 & age_start < 0.08

	// STANDARDIZE SEX
		//drop if sex != 1 & sex != 2

	// LOCATION_ID
		rename location_id location_id_orig
		merge m:1 iso3 using "FILEPATH"
			assert _m != 1
			drop if _m == 2
			drop _m
		replace location_id = location_id_orig if location_id_orig != .
		drop location_id_orig

	// COLLAPSE CASES
		// fastcollapse cases deaths, type(sum) by(iso3 subdiv location_id source year age_* sex platform icd_vers cause_primary)
		//fastcollapse cases deaths, type(sum) by(iso3 subdiv location_id source national year age_* sex platform icd_vers dx_mapped_ dx_ecode_id NID)
		collapse (sum) cases deaths, by(iso3 subdiv location_id source national year age_* sex platform icd_vers dx_mapped_ dx_ecode_id NID) fast

	// modify columns to fit our structure
		rename dx_mapped_ cause_code
		rename dx_ecode_id diagnosis_id

	// SAVE
		compress
		save "FILEPATH", replace
		save "FILEPATH", replace

	capture log close


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
