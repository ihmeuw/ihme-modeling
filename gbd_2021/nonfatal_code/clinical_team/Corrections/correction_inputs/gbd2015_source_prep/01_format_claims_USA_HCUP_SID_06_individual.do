// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author: 		USER
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

			// Define J drive (data) for cluster (UNIX) and Windows (Windows)
				if c(os) == "Unix" {
					global prefix FILEPATH
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix FILEPATH
				}
			// Identify repo for Live birth swap
				local user = "`c(username)'"
				local code_folder FILEPATH
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
				local data_name "USA_HCUP_SID_06"
			// Original data folder
				local input_folder FILEPATH
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
		// Swap Live Birth primary diagnosis subroutine
			// copy dx_1
			clonevar new_dx_1 = dx_1

			// prepare a csv that is stored in this repo that has the live birth codes
			preserve
			import delimited FILEPATH, varn(1) clear
			tempfile live_births_codes
			save `live_births_codes', replace
			restore

			rename dx_1 cause_code
			merge m:1 cause_code icd_vers using `live_births_codes'
			// lose the codes that are only in the icd map
			keep if _merge != 2
			rename cause_code dx_1

			// make a mask that shows where dx_1 has a live birth code
			gen primary_dx_is_live_birth = 1 if _merge == 3
			replace primary_dx_is_live_birth = 0 if primary_dx_is_live_birth == .
			drop _merge

			// make list of numbers representing each of the dx columns
			// NOTE, only the diagnosis columns can be named dx_ for this to work
			local dx_count = 0
			foreach var of varlist dx_* {
				local dx_count = `dx_count' + 1
			}
			display `dx_count'

			//loop over all the dx columns, starting at the highest numbered one
			// E.g., start at dx_14, then do dx_13, etc. the (-1) decrements by 1.
			gen is_live_birth = .
			forvalues dx = `dx_count'(-1)1 {
				replace is_live_birth = .
				display `dx'
				rename dx_`dx' cause_code
				merge m:1 cause_code icd_vers using `live_births_codes'
				// lose the codes that are only in the icd map
				keep if _merge != 2
				rename cause_code dx_`dx'
				replace is_live_birth = 1 if _merge == 3
				replace is_live_birth = 0 if is_live_birth == .
				noisily replace new_dx_1 = dx_`dx' if dx_`dx' != "" & is_live_birth == 0 & primary_dx_is_live_birth == 1
				drop _merge
			}

			// make final replacement
			count if dx_1 != new_dx_1
			drop dx_1
			rename new_dx_1 dx_1
			// drop the two helper columns
			drop is_live_birth primary_dx_is_live_birth

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
