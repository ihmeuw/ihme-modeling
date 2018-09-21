// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author: 		USERNAME
// Date: 			September 2015
// Modified:		--
// Project:		GBD
// Purpose:		Template for formatting NZL_NMDS for the combined hospital database


** **************************************************************************
** CONFIGURATION
** **************************************************************************
	** ****************************************************************
	** Prepare STATA for use
	**
	** This section sets the application perferences.  The local applications
	**	preferences include memory allocation, variables limits, color scheme,
	**	defining the FILENAME, and setting a local for the date.
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
					global prefix "FILENAME"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "FILENAME"
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
				local data_name "NZL_NMDS"
			// Original data folder
				//local input_folder "FILENAME"
				// TODO note this is a temp location, will move to limited use
				local input_folder "FILENAME"
			// Log folder
				local log_folder "FILENAME"
				capture mkdir "`log_folder'"
			// Output folder
				local output_folder "FILENAME"
				local archive_folder "FILENAME"
				capture mkdir "`output_folder'"
				capture mkdir "`archive_folder'"


		** ****************************************************************
		** CREATE LOG
		** ****************************************************************
			capture log close
			log using "FILENAME", replace


** **************************************************************************
** RUN PROGRAGM
** **************************************************************************
	// GET DATA
		clear
		//gen y = .
		foreach y in JAN_JUN JUL_DEC {
			append using "FILENAME"
			//replace y = `y' if y == .
		}

		tempfile EVENTS
		save `EVENTS', replace

		clear
		foreach y in JAN_JUN JUL_DEC {
			append using "FILENAME"
		}

		drop OP_ACDTE 	// Operation/Procedure date
		drop if DIAG_TYP == "O"	// Operation/Procedures

		// icd_vers (string): ICD version - "ICD10", "ICD9_detail"
			gen icd_vers = "ICD10" if inlist(CLIN_SYS, "10", "11", "12", "13", "14")
			replace icd_vers = "ICD9_detail" if CLIN_SYS == "01" | CLIN_SYS == "02" | CLIN_SYS == "06"
			drop CLIN_SYS

		tempfile DIAGS
		save `DIAGS', replace

		// DIAGNOSIS & ECODE VARIABLES
			// Primary dx
				keep if DIAG_TYP == "A"
				rename CLIN_CD dx_
				isid EVENT_ID
				replace DIAG_SEQ = 1
				drop DIAG_TYP
				reshape wide dx_, i(EVENT_ID icd_vers) j(DIAG_SEQ)
				sort EVENT_ID
				tempfile primary
				save `primary', replace

			// Secondary dx's. Only keep first 15 diagnoses
				use `DIAGS', clear
				keep if DIAG_TYP == "B"
				rename CLIN_CD dx_
				sort EVENT_ID DIAG_SEQ
				by EVENT_ID : gen num = _n
				drop DIAG_SEQ DIAG_TYP
				drop if num > 15
				reshape wide dx_, i(EVENT_ID icd_vers) j(num)
				sort EVENT_ID
				tempfile secondary
				save `secondary', replace

			// Ecodes
				use `DIAGS', clear
				keep if DIAG_TYP == "E"
				rename CLIN_CD ecode_
				sort EVENT_ID DIAG_SEQ
				by EVENT_ID : gen num = _n
				drop DIAG_SEQ DIAG_TYP
				reshape wide ecode_, i(EVENT_ID icd_vers) j(num)
				sort EVENT_ID

				merge 1:1 EVENT_ID using `primary', nogen
				merge 1:1 EVENT_ID using `secondary', nogen


		merge 1:1 EVENT_ID using `EVENTS', keep(3) nogen



	// ENSURE ALL VARIABLES ARE PRESENT
		// source (string): source name
			gen source = "`data_name'"
		// year (numeric)
			gen year = string(evendate, "%tdCCYY")
			destring year, replace
			drop evendate
		// NID (numeric)
			// TODO replace this when we get a real NID
			gen NID = -1
			// replace NID = 220786 if year == 2000
			// replace NID = 220787 if year == 2001
			// replace NID = 220788 if year == 2002
			// replace NID = 220789 if year == 2003
			// replace NID = 220790 if year == 2004
			// replace NID = 220791 if year == 2005
			// replace NID = 220792 if year == 2006
			// replace NID = 220793 if year == 2007
			// replace NID = 220794 if year == 2008
			// replace NID = 220795 if year == 2009
			// replace NID = 220796 if year == 2010
			// replace NID = 220797 if year == 2011
			// replace NID = 220798 if year == 2012
			// replace NID = 220799 if year == 2013
			// replace NID = 220800 if year == 2014
			// drop y
		// iso3 (string)
			gen iso3 = "NZL"
		// subdiv (string)
			gen subdiv = ""
		// location_id (numeric)
			gen location_id = .
		// national (numeric): 0 = no, 1 = yes
			gen national = 1
		// age (numeric)
			gen age = .
			replace age = 0 if AGE_AT_DISCHARGE == 0
			replace age = floor(AGE_AT_DISCHARGE / 5) * 5
			replace age = 95 if age > 95
			drop AGE_AT_DISCHARGE
		// frmat (numeric): find the WHO format here "FILENAME"
			gen frmat = .
		// im_frmat (numeric): from the same file as above
			gen im_frmat = .
		// sex (numeric): 1=male 2=female 9=missing
			gen sex = .
			replace sex = 1 if gender == "M"
			replace sex = 2 if gender == "F"
			replace sex = 9 if sex == .
		// platform (string): "Inpatient", "Outpatient"
			gen platform = "Inpatient"
		// patient_id (string)
			// Var already present as patient_id
		// icd_vers (string): ICD version - "ICD10", "ICD9_detail"

		// dx_* (string): diagnoses

		// ecode_* (string): variable if E codes are specifically mentioned

		// Inpatient variables
			// discharges (numeric)
				gen metric_discharges = 1
			// day cases (numeric)
				gen metric_day_cases = 1 if real(los) == 0
			// bed_days (numeric)
				gen metric_bed_days = real(los)
			// deaths (numeric)
				gen metric_deaths = 0
				replace metric_deaths = 1 if EVENT_TYPE == "DD"


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

	// DO FINAL COLLAPSE ON DATA
		collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_* ecode_*) fast

	// SAVE
		compress
		save "FILENAME", replace
		save "FILENAME", replace

	capture log close


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
