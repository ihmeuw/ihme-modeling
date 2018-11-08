
** **************************************************************************
** CONFIGURATION
** **************************************************************************
	** ****************************************************************
	** Prepare STATA for use
	**
	** This section sets the application preferences.  The local applications
	**	preferences include memory allocation, variables limits, color scheme,
	**	defining the J drive (data), and setting a local for the date.
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
					global prefix "{FILEPATH}/j"
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
				local data_name "CAN_DAD_94_09"
			
				local input_folder "$prefix{FILEPATH}/raw"
				local log_folder "$prefix{FILEPATH}/logs"
				capture mkdir "`log_folder'"
			// Output folder
				capture mkdir "$prefix{FILEPATH}/`data_name'"
				capture mkdir "$prefix{FILEPATH}"
				capture mkdir "$prefix{FILEPATH}"
				local output_folder "$prefix{FILEPATH}e"
				local archive_folder "`output_folder'/_archive"
				local wide_format_folder "{FILEPATH}"
				capture mkdir "`archive_folder'"


		** ****************************************************************
		** CREATE LOG
		** ****************************************************************
			capture log close
			log using "`log_folder'/00_format_`today'.log", replace
		
			do "$prefix{FILEPATH}/fastcollapse.ado" 

** **************************************************************************
** RUN FORMATTING
** **************************************************************************
	// GET DATA
		clear
		gen foo = .
		tempfile master_data
		save `master_data', replace
		foreach n of numlist 1994/2009 {
			display in red "Working on `n'"
			use "$prefix{FILEPATH}/uwash_dad`n'_final.dta", clear
			foreach var of varlist * {
				local nn = lower("`var'")
				rename `var' `nn'
			}
			append using `master_data'
			tempfile master_data
			save `master_data', replace
		}

	// ENSURE ALL VARIABLES ARE PRESENT
		// source (string): source name
			gen source = "`data_name'"
		// iso3 (string)
			gen iso3 = "CAN"
		// subdiv (string)
			gen subdiv = "Ontario"
		// location_id (numeric)
			gen location_id = 43866
		// national (numeric): 0 = no, 1 = yes
			gen national = 0
		// year (numeric)
			rename fiscal_year year
			destring year, replace
		// NID (numeric)
			gen NID = .
			replace NID = 86918 if year == 1994
			replace NID = 86919 if year == 1995
			replace NID = 86920 if year == 1996
			replace NID = 86921 if year == 1997
			replace NID = 86922 if year == 1998
			replace NID = 86924 if year == 1999
			replace NID = 86925 if year == 2000
			replace NID = 86926 if year == 2001
			replace NID = 86927 if year == 2002
			replace NID = 86928 if year == 2003
			replace NID = 86929 if year == 2004
			replace NID = 86930 if year == 2005
			replace NID = 86931 if year == 2006
			replace NID = 86932 if year == 2007
			replace NID = 86933 if year == 2008
			replace NID = 86934 if year == 2009
		// age (numeric)
			gen age = .
			replace age = 0 if age_group == "A. 0 to 6 days"
			replace age = .01 if age_group == "B. 7 to 27 days"
			replace age = .1 if age_group == "C. 28 days to 5 months"
			replace age = .1 if age_group == "D. 6 to 11 months"
			replace age = 1 if age_group == "E. 1 to 4 years"
			replace age = 5 if age_group == "F. 5 to 9 years"
			replace age = 10 if age_group == "G. 10 to 14 years"
			replace age = 15 if age_group == "H. 15 to 19 years"
			replace age = 20 if age_group == "I. 20 to 24 years"
			replace age = 25 if age_group == "J. 25 to 29 years"
			replace age = 30 if age_group == "K. 30 to 34 years"
			replace age = 35 if age_group == "L. 35 to 39 years"
			replace age = 40 if age_group == "M. 40 to 44 years"
			replace age = 45 if age_group == "N. 45 to 49 years"
			replace age = 50 if age_group == "O. 50 to 54 years"
			replace age = 55 if age_group == "P. 55 to 59 years"
			replace age = 60 if age_group == "Q. 60 to 64 years"
			replace age = 65 if age_group == "R. 65 to 69 years"
			replace age = 70 if age_group == "S. 70 to 74 years"
			replace age = 75 if age_group == "T. 75 to 79 years"
			replace age = 80 if age_group == "U. 80 to 84 years"
			replace age = 80 if age_group == "W. 80 to 84 years"
			replace age = 85 if age_group == "V. 85+ years"
			drop age_group
		// frmat (numeric):
			gen frmat = 2
		// im_frmat (numeric): from the same file as above
			gen im_frmat = 2
		// sex (numeric): 1=male 2=female 9=missing
			gen sex = 1 if gender_code == "M"
			replace sex = 2 if gender_code == "F"
			drop gender_code
			replace sex = 9 if sex == .
		// platform (string): "Inpatient", "Outpatient"
			gen platform = "Inpatient"

			replace platform = "Inpatient + Outpatient" if year <= 2001
		// patient_id (string)

			rename proj_id patient_id
			tostring(patient_id), replace
			replace patient_id = "" if patient_id == "."
			replace patient_id = "ID"+patient_id if patient_id != ""
			// Generate unique ids for the blank patients
				gen obs = _n
				replace patient_id = "NL"+string(obs) if patient_id == ""
				drop obs

			gen icd_vers = "ICD10"
			replace icd_vers = "ICD9_detail" if year <= 2001
		// dx_* (string): diagnoses
			foreach n of numlist 1/25 {
				rename diag_code_`n' dx_`n'
			}
		// ecode_* (string): variable if E codes are specifically mentioned
			gen ecode_1 = ""
		// Inpatient variables
			// discharges (numeric)
				gen metric_discharges = 1
			// bed_days (numeric)
				gen metric_bed_days = total_los_days
			// deaths (numeric)
				gen metric_deaths = 0
				replace metric_deaths = 1 if discharge_disposition == "07"

			// drop day cases that don't result in deaths
				keep if metric_bed_days > 0 | metric_deaths == 1
	// VARIABLE CHECK

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

	// DO FINAL COLLAPSE ON DATA
		collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_* ecode_*) fast

		preserve
			keep if metric_discharges > 0
			compress
			save "`wide_format_folder'/`data_name'.dta", replace
		restore

** **************************************************************************
** RUN SELECT PRIMARY
** **************************************************************************
	gen cause_primary = dx_1

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
		. gen long id = _n
		reshape long dx_ ecode_, i(id) j(dx_ecode_id)
		replace dx_ecode_id = 2 if dx_ecode_id > 2
		drop if missing(dx_)

	// Collapse
		keep iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_ecode_id metric_* dx_ ecode_

** **************************************************************************
** RUN EPI COMPILE CLUSTER
** **************************************************************************
		// STANDARDIZE PLATFORMS
			replace platform = "Outpatient" if platform == "Emergency"
			replace platform = "1" if platform == "Inpatient" | platform == "Inpatient 1"
			// drop day cases
			drop if platform == "Inpatient 2"
			// drop mixed inp otp data
			drop if platform == "Inpatient + Outpatient"

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
			
		// COLLAPSE CASES
			collapse (sum) cases deaths, by(iso3 subdiv location_id source national year age_* sex platform icd_vers dx_ dx_ecode_id NID) fast

		// modify columns to fit our structure
			rename dx_ cause_code
			rename dx_ecode_id diagnosis_id

		// drop unneeded cols
			drop iso3 subdiv
	// SAVE
		compress
		save "`output_folder'/formatted_CAN_DAD_94_09.dta", replace
		save "`archive_folder'/formatted_CAN_DAD_94_09`today'.dta", replace

	capture log close


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
