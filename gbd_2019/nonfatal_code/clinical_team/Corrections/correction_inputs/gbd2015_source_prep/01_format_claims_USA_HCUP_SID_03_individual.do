// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Date: 			December 23, 2013
// Modified:		--
// Project:		GBD
// Purpose:		Template for formatting USA_HCUP_SID_* for the combined hospital database


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
				local data_name "USA_HCUP_SID_03"
			// Original data folder
				local input_folder FILEPATH
			// Temp folder
				// local temp_folder FILEPATH
				// local temp_folder FILEPATH
				// capture mkdir "`temp_folder'"
				// local temp_folder FILEPATH
				// capture mkdir "`temp_folder'"
			// Code folder
				local code_folder FILEPATH
				//local code_folder FILEPATH
			// Log folder
				local log_folder FILEPATH
				capture mkdir "`log_folder'"
				
			// File name
				global hcup_file_pathway `1'
				global hcup_save_pathway `2'
				global file_name `3'


		** ****************************************************************
		** CREATE LOG
		** ****************************************************************
			capture log close
			log using "`log_folder'/00_format_`today'_${file_name}.log", replace


** **************************************************************************
** RUN PROGRAGM
** **************************************************************************
	// Get data
		use "$hcup_file_pathway", clear
	
	// Make all variable names lowercase
		foreach var of varlist * {
			local nn = lower("`var'")
			rename `var' `nn'
		}

	// ENSURE ALL VARIABLES ARE PRESENT
		foreach target_var in totchg zipinc_q npr los hcup_ed drg pr1 ageday {
			capture gen `target_var' = .
		}
		capture gen hospst = substr(substr("${file_name}",1,6),-2,.)
		lookfor pnum_r
		local p_count = 0
		foreach i in `r(varlist)' {
			local p_count = `p_count' + 1
		}
		lookfor ayear
		local y_count = 0
		foreach i in `r(varlist)' {
			local y_count = `y_count' + 1
		}
		lookfor amonth
		local m_count = 0
		foreach i in `r(varlist)' {
			local m_count = `m_count' + 1
		}
		if `p_count' == 0 {
			gen pnum_r = .
		}
		if `y_count' == 0 {
			gen ayear = .
		}
		if `m_count' == 0 {
			gen amonth = .
		}
		keep year age* dx* ecode* dispunif female totchg zipinc_q npr los hcup_ed drg* pr* hospst died key pnum_r amonth ayear
		// source (string): source name
			gen source = "`data_name'_${file_name}"
		// NID (numeric)
			gen NID = .
		// iso3 (string)
			gen iso3 = "USA"
		// subdiv (string)
			gen subdiv = hospst
		// location_id (numeric)
			gen location_id = .
		// national (numeric): 0 = no, 1 = yes
			gen national = 0
		// year (numeric)
			tostring(year), replace
			replace year = substr(substr("${file_name}",-13,.),1,4)
			destring(year), force replace
		// age (numeric)
			// gen age_start=.

			// replace age_start = 0 if age == 0

			// replace age_start = 0 if ageday <= 6 & age == 0
			// replace age_start = .01 if ageday >= 7 & ageday <= 27 & age == 0
			// replace age_start = .1 if ageday >= 28 & ageday <=365 & age == 0
			// replace age_start = 1 if (age >=1 & age <=4) 
			// forvalues a = 5(5)90 {
			// 	replace age_start = `a' if age >= `a' & age <= (`a' + 4)
			// }
			// replace age_start = 95 if age >= 95
			// drop age
			// rename age_start age
		// frmat (numeric): find the WHO format here FILEPATH
			gen frmat = 2
		// im_frmat (numeric): from the same file as above
			gen im_frmat = 2
		// sex (numeric): 1=male 2=female 9=missing
			gen sex = 2 if female == 1
			replace sex = 1 if female == 0
			replace sex = 9 if sex == .
			drop female
		// platform (string): "Inpatient", "Outpatient", "ED"
			gen platform = "Inpatient"
		// patient_id (string)
			rename pnum_r patient_id
			tostring(patient_id), replace
		// icd_vers (string): ICD version - "ICD10", "ICD9_detail"
			gen icd_vers = "ICD9_detail"
		// dx* (string): diagnoses
			forvalues n = 1(1)100 {
				capture rename dx`n' dx_`n'
			}
			capture gen dx_1 = ""
		// ecode* (string): variable if E codes are specifically mentioned
			forvalues n = 1(1)100 {
				capture rename ecode`n' ecode_`n'
			}
			capture gen ecode_1 = ""
		// Inpatient variables
			// total charges
				rename totchg metric_total_charges
			// discharges (numeric)
				gen metric_discharges = 1
			// bed_days (numeric)
				rename los metric_bed_days
			// deaths
				gen metric_deaths = 0
				replace metric_deaths = 1 if died == 1


	// VARIABLE CHECK
		// If any of the variables in our template are missing, create them now (even if they are empty)
		// All of the following variables should be present
			#delimit;
			order
			amonth ayear
			iso3 subdiv location_id national
			source NID
			year
			age frmat im_frmat
			sex platform patient_id
			icd_vers dx_* ecode_*
			metric_*;
		// Drop any variables not in our template of variables to keep
			keep
			amonth ayear
			iso3 subdiv location_id national
			source NID
			year
			age frmat im_frmat
			sex platform patient_id
			icd_vers dx_* ecode_*
			metric_*;
			#delimit cr

	// DO FINAL COLLAPSE ON DATA
		//collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_* ecode_*) fast

	// SAVE
		compress
		save "${hcup_save_pathway}", replace

	capture log close


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
