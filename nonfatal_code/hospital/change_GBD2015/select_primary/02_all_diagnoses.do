// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author: 		USERNAME
// Date:
// Modified:
// Project:		GBD
// Purpose:		Map hospital data source


** **************************************************************************
** CONFIGURATION
** **************************************************************************
	** ****************************************************************
	** Prepare STATA for use
	**
	** This section sets the application preferences.  The local applications
	**	preferences include memory allocation, variables limits, color scheme,
	**	defining the FILEPATH, and setting a local for the date.
	**
	** ****************************************************************
		// Set application preferences
			// Clear memory and set memory and variable limits
				capture restore
				clear all
				set mem 1G
				set maxvar 32000

			// Set to run all selected code without pausing
				set more off

			// Set graph output color scheme
				set scheme s1color

			// Define FILEPATH for cluster (UNIX) and Windows (Windows)
				if c(os) == "Unix" {
					global prefix "FILEPATH"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "FILEPATH"
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
		**	mapping prep.
		**
		** ****************************************************************
			// Data Source Name
				local data_name `1'
			// Original data folder
				local input_folder "FILEPATH"
			// Log folder
				local log_folder "FILEPATH"
				capture mkdir "FILEPATH"
			// Output folder
				local output_folder "FILEPATH"
				local archive_folder "FILEPATH"
				capture mkdir "FILEPATH"
				capture mkdir "FILEPATH"


	** ****************************************************************
	** CREATE LOG
	** ****************************************************************
		capture log close
		log using "FILEPATH", replace


	** ****************************************************************
	** GET GBD RESOURCES
	** ****************************************************************


** **************************************************************************
** RUN PROGRAGM
** **************************************************************************
	// Get hospital data
		use "FILEPATH", clear

	// Select primary GBD code
	** NOTE: use the primary GBD code unless there is an E code
		// gen yld_cause_primary = dx_1_yld_cause
		gen cause_primary = dx_1_mapped
		// Prioritize External Cause of Injury & Service Codes over Nature of Injury
			// Diagnosis codes
				local dx_count = 0
				foreach var of varlist dx_*_original {
					local dx_count = `dx_count' + 1
				}
				local dx_count = `dx_count'
				// Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes
					forvalues dx = `dx_count'(-1)1 {
						display "Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes in dx_`dx'"
						//replace yld_cause_primary = dx_`dx'_yld_cause if (inlist(substr(dx_`dx'_mapped,1,1),"V","W","X","Y") & icd_vers == "ICD10") | (inlist(substr(dx_`dx'_mapped,1,1),"E") & icd_vers == "ICD9_detail")
						replace cause_primary = dx_`dx'_mapped if (inlist(substr(dx_`dx'_mapped,1,1),"V","W","X","Y") & icd_vers == "ICD10") | (inlist(substr(dx_`dx'_mapped,1,1),"E") & icd_vers == "ICD9_detail")
					}
			// External cause set
				local ecode_count = 0
				foreach var of varlist ecode_*_original {
					local ecode_count = `ecode_count' + 1
				}
				local ecode_count = `ecode_count'
				// Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes
					forvalues dx = `ecode_count'(-1)1 {
						display "Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes in ecode_`dx'"
						//replace yld_cause_primary = ecode_`dx'_yld_cause if (inlist(substr(ecode_`dx'_mapped,1,1),"V","W","X","Y") & icd_vers == "ICD10") | (inlist(substr(ecode_`dx'_mapped,1,1),"E") & icd_vers == "ICD9_detail")
						replace cause_primary = ecode_`dx'_mapped if (inlist(substr(ecode_`dx'_mapped,1,1),"V","W","X","Y") & icd_vers == "ICD10") | (inlist(substr(ecode_`dx'_mapped,1,1),"E") & icd_vers == "ICD9_detail")
					}
	// Reshape long
		drop dx_1_mapped
		rename cause_primary dx_1_mapped
		rename dx_*_mapped dx_mapped_*
		rename ecode_*_mapped ecode_mapped_*
		//reshape long dx_mapped_ ecode_mapped_, i(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers yld_cause_primary cause_primary) j(dx_num)
		// stata kept saying we specified too many vars for reshape or that smaller # weren't unique. so we explicitly created an index
		. gen long id = _n
		reshape long dx_mapped_ ecode_mapped_, i(id) j(dx_ecode_id)
		replace dx_ecode_id = 2 if dx_ecode_id > 2
		drop if missing(dx_mapped_)
		//drop id

	// Collapse
		keep iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_ecode_id metric_* dx_mapped_* ecode_mapped_*
		collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_ecode_id dx_mapped* ecode_mapped_*) fast

	// Save
		compress
		//save "FILEPATH", replace
		save "FILEPATH", replace
		save "FILEPATH", replace


	capture log close


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
