
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
				cap set maxvar 32000

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
				local data_name "BRA_SIH"
			// Original data folder
				local input_folder "$prefix{FILEPATH}/raw"
			// Log folder
				local log_folder "$prefix{FILEPATH}/logs"
				capture mkdir "`log_folder'"
			// Output folder
				local output_folder "$prefix{FILEPATH}"
				local archive_folder "`output_folder'/_archive"
				local wide_format_folder "{FILEPATH}"
				capture mkdir "`output_folder'"
				capture mkdir "`archive_folder'"


		** ****************************************************************
		** CREATE LOG
		** ****************************************************************
			capture log close
			log using "`log_folder'/00_format_`today'.log", replace


** **************************************************************************
** RUN FORMATTING PROGRAGM
** **************************************************************************
	// GET DATA
		use "`input_folder'/BRA_SIH_1998_2014.dta", clear
	
	// VARIABLE CHECK
		// If any of the variables in our template are missing, create them now (even if they are empty)
		// SOURCE
			replace source = "`data_name'"
		// YEAR
			drop if year < 1997
			assert year != .
		// NID
			replace NID = 221323 if year == 2014
			replace NID = 104258 if year == 2013
			replace NID = 104257 if year == 2012
			replace NID = 104256 if year == 2011
			replace NID = 104255 if year == 2010
			replace NID = 26333 if year == 2009
			replace NID = 87012 if year == 2008
			replace NID = 87013 if year == 2007
			replace NID = 87014 if year == 2006
			replace NID = 104254 if year == 2005
			replace NID = 104253 if year == 2004
			replace NID = 104252 if year == 2003
			replace NID = 104251 if year == 2002
			replace NID = 104250 if year == 2001
			replace NID = 104249 if year == 2000
			replace NID = 104248 if year == 1999
			replace NID = 104247 if year == 1998
			replace NID = 104246 if year == 1997
			assert NID != .
		// frmat (numeric): find the WHO format here "J:{FILEPATH}/Age formats documentation.xlsx"
			drop frmat
			gen frmat = 2
		// im_frmat (numeric): from the same file as above
			drop im_frmat
			gen im_frmat = 2
		
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

		preserve
			keep if metric_discharges > 0
			compress
			save "`wide_format_folder'/`data_name'.dta", replace
		restore

** **************************************************************************
** RUN SELECT PRIMARY PROGRAGM
** **************************************************************************
	// Select primary GBD code
	
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
		rename cause_primary dx_1

		// drop the ecode cols now that we've prioritized
		drop ecode_*

		. gen long id = _n
		reshape long dx_, i(id) j(dx_ecode_id)
		replace dx_ecode_id = 2 if dx_ecode_id > 2
		drop if missing(dx_)
		//drop id

	// Collapse
		keep iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_ecode_id metric_* dx_*
		collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_ecode_id dx*) fast

** **************************************************************************
** RUN EPI COMPILE CLUSTER PROGRAGM
** **************************************************************************
	
	// STANDARDIZE PLATFORMS
		replace platform = "Outpatient" if platform == "Emergency"

		replace platform = "1" if platform == "Inpatient" | platform == "Inpatient 1"
		// drop day cases
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
	collapse (sum) cases deaths, by(iso3 subdiv location_id source national year age_* sex platform icd_vers dx_ dx_ecode_id NID) fast
		// SAVE

	// modify columns to fit our structure
		rename dx_ cause_code
		rename dx_ecode_id diagnosis_id

	// drop unneeded cols
	drop iso3 subdiv

	compress
	save "`output_folder'/formatted_BRA_SIH.dta", replace
	save "`archive_folder'/formatted_BRA_SIH_`today'.dta", replace

	capture log close
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
