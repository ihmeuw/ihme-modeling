

** **************************************************************************
** CONFIGURATION
** **************************************************************************
	** ****************************************************************
	** Prepare STATA for use
	**
	** This section sets the application perferences.  The local applications
	**	preferences include memory allocation, variables limits, color scheme,
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
				local input_folder "filepath"
			// Log folder
				local log_folder "filepath"
				capture mkdir "`log_folder'"
			// Output folder
				local output_folder "filepath"
				local archive_folder "`filepath"


		** ****************************************************************
		** CREATE LOG
		** ****************************************************************
			capture log close
			log using "`log_folder'filepath", replace

	

** **************************************************************************
** RUN FORMAT PROGRAGM
** **************************************************************************
	// GET DATA
		clear
		gen y = .
		forvalues y = 2000/2014 {
			append using "`filepath"
			replace y = `y' if y == .
		}
		// now add 2015 which follows a different naming format
		local year = 2015
		foreach month in JAN_JUN JUL_DEC {
			append using "`filepath"
			replace y = `year' if y == .
		}
		
		tempfile EVENTS
		save `EVENTS', replace
		
		clear
		forvalues y = 2000/2014 {
			append using "`filepath"
		}
	
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
			gen NID = .
			replace NID = 220786 if year == 2000
			replace NID = 220787 if year == 2001
			replace NID = 220788 if year == 2002
			replace NID = 220789 if year == 2003
			replace NID = 220790 if year == 2004
			replace NID = 220791 if year == 2005
			replace NID = 220792 if year == 2006
			replace NID = 220793 if year == 2007
			replace NID = 220794 if year == 2008
			replace NID = 220795 if year == 2009
			replace NID = 220796 if year == 2010
			replace NID = 220797 if year == 2011
			replace NID = 220798 if year == 2012
			replace NID = 220799 if year == 2013
			replace NID = 220800 if year == 2014
			replace NID = 293984 if year == 2015
			drop y
		// iso3 (string)
			gen iso3 = "NZL"
		// subdiv (string)
			gen subdiv = ""
		// location_id (numeric)
			gen location_id = .
		// national (numeric): 0 = no, 1 = yes
			gen national = 1
		// admission date
			rename evstdate adm_date
		// age (numeric)
			rename AGE_AT_DISCHARGE age
			// gen age = .
			// replace age = 0 if AGE_AT_DISCHARGE == 0
			// replace age = 1 if AGE_AT_DISCHARGE >=1 & AGE_AT_DISCHARGE <=4
			// forvalues a = 5(5)90 {
			// 	replace age = `a' if AGE_AT_DISCHARGE >= `a' & AGE_AT_DISCHARGE <= (`a' + 4)
			// }
			// replace age = 95 if AGE_AT_DISCHARGE >= 95
			
			// this method was not correct
			// gen age = .
			// replace age = 0 if AGE_AT_DISCHARGE == 0
			// replace age = floor(AGE_AT_DISCHARGE / 5) * 5
			// replace age = 95 if age > 95
			// drop AGE_AT_DISCHARGE
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
				replace metric_deaths = 1 if END_TYPE == "DD"
		// Keep only inpatient 24 hour stays ie drop day cases
			keep if real(los) != 0

	// VARIABLE CHECK
		// If any of the variables in our template are missing, create them now (even if they are empty)
		// All of the following variables should be present
			#delimit;
			order
			adm_date
			iso3 subdiv location_id national
			source NID
			year
			age frmat im_frmat
			sex platform patient_id
			icd_vers dx_* ecode_*
			metric_*;
		// Drop any variables not in our template of variables to keep
			keep
			adm_date
			iso3 subdiv location_id national
			source NID
			year
			age frmat im_frmat
			sex platform patient_id
			icd_vers dx_* ecode_*
			metric_*;
			#delimit cr

	// COLLAPSE DATA
		//collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_* ecode_*) fast

** **************************************************************************
** RUN SELECT PRIMARY PROGRAGM
** **************************************************************************

	// Reshape long
		//drop dx_*_original dx_*_yld_cause ecode_*_yld_cause ecode_*_original
		// drop dx_1
		// rename cause_primary dx_1

		// drop Ecodes since we've already replaced dx_1 with an ecode where appropriate
		drop ecode_*
		
		. gen long id = _n
		reshape long dx_, i(id) j(dx_ecode_id)
		replace dx_ecode_id = 2 if dx_ecode_id > 2
		drop if missing(dx_)
		//drop id

	// Collapse

	
** **************************************************************************
** RUN EPI COMPILE CLUSTER PROGRAGM
** **************************************************************************

	// STANDARDIZE PLATFORMS
		replace platform = "Outpatient" if platform == "Emergency"
		replace platform = "1" if platform == "Inpatient" | platform == "Inpatient 1"
		drop if platform == "Inpatient 2"
		replace platform = "2" if platform == "Outpatient"
		destring platform, replace
		assert platform != .
	

	// LOCATION_ID
		rename location_id location_id_orig
		merge m:1 iso3 using "filepath"
			assert _m != 1
			drop if _m == 2
			drop _m
		replace location_id = location_id_orig if location_id_orig != .
		drop location_id_orig

	// COLLAPSE CASES
		// fastcollapse cases deaths, type(sum) by(iso3 subdiv location_id source year age_* sex platform icd_vers cause_primary)
		// fastcollapse discharges deaths, type(sum) by(iso3 subdiv location_id source national year age_* sex platform icd_vers dx_ dx_ecode_id NID)
		//collapse (sum) cases deaths, by(iso3 subdiv location_id source national year age_* sex platform icd_vers dx_ dx_ecode_id NID) fast

	// modify columns to fit our structure
		rename dx_ cause_code
		rename dx_ecode_id diagnosis_id

	// SAVE
		compress
		save "`output_folder", replace



// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
