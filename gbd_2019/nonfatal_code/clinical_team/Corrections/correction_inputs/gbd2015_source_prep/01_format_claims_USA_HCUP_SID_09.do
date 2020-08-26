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
	**	and setting a local for the date.
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
				local data_name "USA_HCUP_SID_09"
			// Start/End year
				local year_start 2009
				local year_end 2009
			// Original data folder
				local input_folder FILEPATH
			// Temp folder
				local temp_folder FILEPATH
				capture mkdir "`temp_folder'"
				local temp_folder FILEPATH
				capture mkdir "`temp_folder'"
				local temp_folder FILEPATH
				capture mkdir "`temp_folder'"
			// Code folder
				local code_folder FILEPATH
				//local code_folder FILEPATH
			// Log folder
				local log_folder FILEPATH
				capture mkdir "`log_folder'"
			// Output folder
			// 	local output_folder FILEPATH
			// 	local archive_folder "`output_folder'/_archive"
			// 	capture mkdir "`output_folder'"
			// 	capture mkdir "`archive_folder'"
			// Output folder
				local output_folder FILEPATH
				local archive_folder "`output_folder'/_archive"
				capture mkdir "`output_folder'"
				capture mkdir "`archive_folder'"


		** ****************************************************************
		** CREATE LOG
		** ****************************************************************
			capture log close
			log using "`log_folder'/00_format_`today'.log", replace


** **************************************************************************
** RUN PROGRAGM
** **************************************************************************
	// Clean tempfile folder
		local hcup_files: dir "`temp_folder'/" files "*", respectcase
		foreach file of local hcup_files {
			capture rm "`temp_folder'/`file'"
		}
		
	// Create jobs to split data sets into temporary files which can then be aggregated
		foreach n of numlist `year_start'/`year_end' {
			local hcup_core`n': dir "`input_folder'/`n'" files "*CORE.DTA", respectcase
			
			foreach file of local hcup_core`n' {
				// Set locals
					local hcup_file_pathway "`input_folder'/`n'/`file'"
					local hcup_save_pathway "`temp_folder'/`file'"
				// Submit Jobs
					QSUB
			}
		}

	// Append all datasets
		clear
		foreach n of numlist `year_start'/`year_end' {
			local hcup_core`n': dir "`input_folder'/`n'" files "*CORE.DTA", respectcase
			
			foreach file of local hcup_core`n' {
				local checkfile "`temp_folder'/`file'"
				display "Looking for `checkfile'"
				capture confirm file "`checkfile'"
				if _rc == 0 {
					display "FOUND!"
				}
				while _rc == 601 {
					sleep 30000
					capture confirm file "`checkfile'"
					if _rc == 0 {
						display "FOUND!"
						sleep 500
					}
				}
				display "Appending on `file'"
				append using "`temp_folder'/`file'", force
			}
		}

	// ENSURE ALL VARIABLES ARE PRESENT
		// source (string): source name
			replace source = "`data_name'"
		// NID (numeric)
			capture gen NID = .
			replace NID = 90320 if year == 1997
			replace NID = 90321 if year == 2000
			replace NID = 90314 if year == 2003
			replace NID = 90315 if year == 2004
			replace NID = 90316 if year == 2005
			replace NID = 90317 if year == 2006
			replace NID = 90318 if year == 2007
			replace NID = 90319 if year == 2008
			replace NID = 90322 if year == 2009
		// iso3 (string)
			** already defined
		// subdiv (string)
			** replace subdiv = ""
		// location_id (numeric)
			** already defined
			// AZC 12.11.2015: Fix location_id for subnational
			replace location_id = 527 if subdiv == "CA"
			replace location_id = 528 if subdiv == "CO"
			replace location_id = 532 if subdiv == "FL"
			replace location_id = 538 if subdiv == "IA"
			replace location_id = 543 if subdiv == "MD"
			replace location_id = 524 if subdiv == "MA"
			replace location_id = 545 if subdiv == "MI"
			replace location_id = 551 if subdiv == "NV"
			replace location_id = 553 if subdiv == "NJ"
			replace location_id = 555 if subdiv == "NY"
			replace location_id = 556 if subdiv == "NC"
			replace location_id = 570 if subdiv == "WA"
			replace location_id = 572 if subdiv == "WI"

			replace location_id = 525 if subdiv == "AZ"
			replace location_id = 526 if subdiv == "AR"

	// DO FINAL COLLAPSE ON DATA
		//collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_* ecode_*) fast


** **************************************************************************
** RUN SELECT PRIMARY PROGRAM
** **************************************************************************
	// Select primary GBD code
	** NOTE: use the primary GBD code unless there is an E code
		// gen yld_cause_primary = dx_1_yld_cause
		// gen cause_primary = dx_1
		// // Prioritize External Cause of Injury & Service Codes over Nature of Injury
		// 	// Diagnosis codes
		// 		local dx_count = 0
		// 		foreach var of varlist dx_* {
		// 			local dx_count = `dx_count' + 1
		// 		}
		// 		local dx_count = `dx_count'
		// 		// Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes
		// 			forvalues dx = `dx_count'(-1)1 {
		// 				display "Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes in dx_`dx'"
		// 				//replace yld_cause_primary = dx_`dx'_yld_cause if (inlist(substr(dx_`dx',1,1),"V","W","X","Y") & icd_vers == "ICD10") | (inlist(substr(dx_`dx',1,1),"E") & icd_vers == "ICD9_detail")
		// 				replace cause_primary = dx_`dx' if (inlist(substr(dx_`dx',1,1),"V","W","X","Y") & icd_vers == "ICD10") | (inlist(substr(dx_`dx',1,1),"E") & icd_vers == "ICD9_detail")
		// 			}
		// 	// External cause set
		// 		local ecode_count = 0
		// 		foreach var of varlist ecode_* {
		// 			local ecode_count = `ecode_count' + 1
		// 		}
		// 		local ecode_count = `ecode_count'
		// 		// Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes
		// 			forvalues dx = `ecode_count'(-1)1 {
		// 				display "Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes in ecode_`dx'"
		// 				//replace yld_cause_primary = ecode_`dx'_yld_cause if (inlist(substr(ecode_`dx',1,1),"V","W","X","Y") & icd_vers == "ICD10") | (inlist(substr(ecode_`dx',1,1),"E") & icd_vers == "ICD9_detail")
		// 				replace cause_primary = ecode_`dx' if (inlist(substr(ecode_`dx',1,1),"V","W","X","Y") & icd_vers == "ICD10") | (inlist(substr(ecode_`dx',1,1),"E") & icd_vers == "ICD9_detail")
		// 			}

	// Prep for reshape long
		tempfile pre_reshape
		save `pre_reshape', replace

		assert location_id != .
		levelsof location_id, local(locs)
		foreach loc of local locs {
			use `pre_reshape', clear
			keep if location_id == `loc'

			// the reshape is very slow with 30 vars and sometimes they're blank, so drop any columns that consist of nothing but missing variables
			foreach var of varlist dx_* {
				capture assert mi(`var')
				if !_rc {
					drop `var'
				}
			}

		// Reshape long
			// drop dx_1
			// rename cause_primary dx_1

			// drop the ecode cols now that we've prioritized
			drop ecode_*

			//rename dx_* dx_*
			//rename ecode_* ecode_*
			//reshape long dx_ ecode_, i(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers yld_cause_primary cause_primary) j(dx_num)
			// stata kept saying we specified too many vars for reshape or that smaller # weren't unique. so we explicitly created an index
			. gen long id = _n
			reshape long dx_, i(id) j(dx_ecode_id)
			replace dx_ecode_id = 2 if dx_ecode_id > 2
			drop if missing(dx_)
			//drop id

		// Collapse
			// keep iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_ecode_id metric_* dx_*
			// collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_ecode_id dx_*) fast

			tempfile `loc'
			save ``loc''
		} // end reshape long loop

		clear
		foreach loc_data of local locs {
			append using ``loc_data''
		}

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
	// 	gen cases = .
	// 		capture confirm var metric_discharges
	// 		if !_rc {
	// 			replace cases = metric_discharges if platform == 1
	// 		}
	// 		capture confirm var metric_discharges_weighted
	// 		if !_rc {
	// 			replace cases = metric_discharges_weighted if platform == 1
	// 		}
	// 		capture confirm var metric_visits
	// 		if !_rc {
	// 			replace cases = metric_visits if platform == 2
	// 		}
	// 		capture confirm var metric_visits_weighted
	// 		if !_rc {
	// 			replace cases = metric_visits_weighted if platform == 2
	// 		}
	// 	gen deaths = .
	// 		capture confirm var metric_deaths
	// 		if !_rc {
	// 			replace deaths = metric_deaths
	// 		}
	// 		capture confirm var metric_deaths_weighted
	// 		if !_rc {
	// 			replace deaths = metric_deaths_weighted
	// 		}
		
	// // STANDARDIZE AGES
	// 	drop if age == .
	// 	replace age = 95 if age > 95
	// 	replace age = 0 if age < 1

	// 	rename age age_start
	// 	gen age_end = age_start + 4
	// 	replace age_end = 1 if age_start == 0
	// 	replace age_end = 99 if age_start == 95
	// 	replace age_end = 4 if age_start == 1

		
	// STANDARDIZE SEX
		//drop if sex != 1 & sex != 2
		
	// LOCATION_ID
		rename location_id location_id_orig
		merge m:1 iso3 using FILEPATH
			assert _m != 1
			drop if _m == 2
			drop _m
		replace location_id = location_id_orig if location_id_orig != .
		drop location_id_orig
		
// COLLAPSE CASES
	//collapse (sum) cases deaths, by(iso3 subdiv location_id source national year age_* sex platform icd_vers dx_ dx_ecode_id NID) fast

// modify columns to fit our structure
	rename dx_ cause_code
	rename dx_ecode_id diagnosis_id

// write by year and location
	tempfile pre_write
	save `pre_write', replace

	assert location_id != .
	levelsof location_id, local(locs)
	foreach loc of local locs {
		use `pre_write', clear
		keep if location_id == `loc'
	// SAVE
		compress
		save "`output_folder'
		save "`archive_folder'
	}
	capture log close


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
