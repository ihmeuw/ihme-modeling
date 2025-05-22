// THIS FORMATTING SCRIPT SHOULD NOT BE RUN. IF THE TEAM NEEDS TO RE-PREP NHDS DATA THEN A
// NEW PYTHON FORMATTING PROCESS SHOULD BE USED!
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author: 		USERNAME
// Date: 			September 7, 2012
// Modified:		September 1, 2013 - Modified to fit standard hospital database variables
// Project:		GBD
// Purpose:		Template for formatting USA_NHDS_79_10 for the combined hospital database


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
					global prefix "/home/j"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "J:"
				}

			// Identify repo for Live birth swap
				local user = "`c(username)'"
				local code_folder "FILEPATH"

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
				local data_name "USA_NHDS_79_10"
			// Original data folder
				local input_folder "FILEPATH"
			// Log folder
				local log_folder "FILEPATH"
				capture mkdir "`log_folder'"
			// Output folder
				local output_folder "FILEPATH"
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
	// GET DATA
		use "`input_folder'/USA_NHDS_79_10.dta", clear

	// ENSURE ALL VARIABLES ARE PRESENT
		// source (string): source name
			gen source = "`data_name'"
		// NID (numeric)
			gen NID = .
			replace NID = 86901 if year == 1979
			replace NID = 86902 if year == 1980
			replace NID = 86903 if year == 1981
			replace NID = 86904 if year == 1982
			replace NID = 86905 if year == 1983
			replace NID = 86906 if year == 1984
			replace NID = 86907 if year == 1985
			replace NID = 86908 if year == 1986
			replace NID = 86909 if year == 1987
			replace NID = 86910 if year == 1988
			replace NID = 86911 if year == 1989
			replace NID = 86912 if year == 1990
			replace NID = 86913 if year == 1991
			replace NID = 86914 if year == 1992
			replace NID = 86915 if year == 1993
			replace NID = 86916 if year == 1994
			replace NID = 86917 if year == 1995
			replace NID = 86886 if year == 1996
			replace NID = 86887 if year == 1997
			replace NID = 86888 if year == 1998
			replace NID = 86889 if year == 1999
			replace NID = 86890 if year == 2000
			replace NID = 86891 if year == 2001
			replace NID = 86892 if year == 2002
			replace NID = 86893 if year == 2003
			replace NID = 86894 if year == 2004
			replace NID = 86895 if year == 2005
			replace NID = 86896 if year == 2006
			replace NID = 86897 if year == 2007
			replace NID = 86898 if year == 2008
			replace NID = 86899 if year == 2009
			replace NID = 86900 if year == 2010
		// iso3 (string)
			gen iso3 = "USA"
		// subdiv (string)
			gen subdiv = ""
		// location_id (numeric)
			gen location_id = 102
		// national (numeric): 0 = no, 1 = yes
			gen national = 1
		// year (numeric)
			** variable already exists
		// age (numeric)
			gen age_start=.
			replace age_start = 0 if age == 0
			replace age_start = 1 if age >=1 & age <=4
			replace age_start = 5 if age >= 5 & age <=9
			replace age_start = 10 if age >=10 & age <= 14
			replace age_start = 15 if age >=15 & age <= 19
			replace age_start = 20 if age >=20 & age <= 24
			replace age_start = 25 if age >=25 & age <= 29
			replace age_start = 30 if age >=30 & age <= 34
			replace age_start = 35 if age >=35 & age <= 39
			replace age_start = 40 if age >=40 & age <= 44
			replace age_start = 45 if age >=45 & age <= 49
			replace age_start = 50 if age >=50 & age <= 54
			replace age_start = 55 if age >=55 & age <= 59
			replace age_start = 60 if age >=60 & age <= 64
			replace age_start = 65 if age >=65 & age <= 69
			replace age_start = 70 if age >=70 & age <= 74
			replace age_start = 75 if age >=75 & age <= 79
			replace age_start = 80 if age >=80 & age <= 84
			replace age_start = 85 if age >=85 & age <= 89
			replace age_start = 90 if age >=90 & age <= 94
			replace age_start = 95 if age >= 95
			replace age_start = -99 if age == .
			replace age_start = 0 if ageunits == 3 & age == 0
			replace age_start = 0 if ageunits == 3 & age >= 1 & age <= 6
			replace age_start = 0 if ageunits == 3 & age >= 7 & age <= 27
			replace age_start = 0 if ageunits == 2 | (ageunits == 3 & age >= 28)
			assert missing(age_start) == 0
			drop age ageunits
			rename age_start age
		// frmat (numeric): find the WHO format here "J:\WORK\03_cod\02_datasets\programs\agesex_splitting\documentation\Age formats documentation.xlsx"
			gen frmat = 2
		// im_frmat (numeric): from the same file as above
			gen im_frmat = 2
		// sex (numeric): 1=male 2=female 9=missing
			** variable already exists
		// platform (string): "Inpatient", "Outpatient", "ED"
			gen platform = "Inpatient 1"
			replace platform = "Inpatient 2" if losflag == 0
		// patient_id (string)
			gen patient_id = ""
		// icd_vers (string): ICD version - "ICD10", "ICD9_detail"
			gen icd_vers = "ICD9_detail"
		// dx* (string): diagnoses
			// Drop dx0 (diganosis at admission)
				drop dx0
			// Rename diagnosis codes
				forvalues n = 1(1)15 {
					display in red "Renaming dx`n'"
					rename dx`n' dx_`n'
				}
			// Clean up diagnosis codes
				foreach var of varlist dx_* {
					display in red "Cleaning up `var'"
					replace `var' = subinstr(subinstr(`var',".","",.),"-","",.)
				}
		// ecode* (string): variable if E codes are specifically mentioned
			gen ecode_1 = ""
		// Inpatient variables
			// discharges (numeric)
				gen metric_discharges_unweighted = 1
			// bed_days (numeric)
				gen metric_bed_days_unweighted = doc
			// day_cases (numeric)
				gen metric_day_cases_unweighted = 0
				replace metric_day_cases_unweighted = 1 if losflag == 0
			// deaths (numeric)
				gen metric_deaths_unweighted = 0
				replace metric_deaths_unweighted = 1 if discstat == 6
			foreach m in discharges bed_days day_cases deaths {
				gen double metric_`m'_weighted = metric_`m'_unweighted * weight
			}

		// Swap Live Birth primary diagnosis subroutine
			// copy dx_1
			clonevar new_dx_1 = dx_1

			// prepare a csv that is stored in this repo that has the live birth codes
			// TODO: CCPURPLE-935: We now package the table as a constant in live_births.
			// Use the Python functions to perform this swap in the refactored formatting code.
			preserve
			import delimited "FILEPATH/live_birth_icd_codes.csv", varn(1) clear
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

	// DO FORMATTING COLLAPSE ON DATA
		collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_* ecode_*) fast

** **************************************************************************
** RUN SELECT PRIMARY PROGRAGM
** **************************************************************************
    // Select primary GBD code
    ** NOTE: use the primary GBD code unless there is an E code (per Mohsen 2013_12_23)
        // gen yld_cause_primary = dx_1_yld_cause
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
                        //replace yld_cause_primary = dx_`dx'_yld_cause if (inlist(substr(dx_`dx'_mapped,1,1),"V","W","X","Y") & icd_vers == "ICD10") | (inlist(substr(dx_`dx'_mapped,1,1),"E") & icd_vers == "ICD9_detail")
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
                        //replace yld_cause_primary = ecode_`dx'_yld_cause if (inlist(substr(ecode_`dx'_mapped,1,1),"V","W","X","Y") & icd_vers == "ICD10") | (inlist(substr(ecode_`dx'_mapped,1,1),"E") & icd_vers == "ICD9_detail")
                        replace cause_primary = ecode_`dx' if (inlist(substr(ecode_`dx',1,1),"V","W","X","Y") & icd_vers == "ICD10") | (inlist(substr(ecode_`dx',1,1),"E") & icd_vers == "ICD9_detail")
                    }
    // Reshape long
        //drop dx_*_original dx_*_yld_cause ecode_*_yld_cause ecode_*_original
        drop dx_1
        rename cause_primary dx_1

        // drop Ecodes since we've already replaced dx_1 with an ecode where appropriate
        drop ecode_*
        //rename dx_*_mapped dx_mapped_*
        //rename ecode_*_mapped ecode_mapped_*
        //reshape long dx_mapped_ ecode_mapped_, i(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers yld_cause_primary cause_primary) j(dx_num)
        // stata kept saying we specified too many vars for reshape or that smaller # weren't unique. so we explicitly created an index
        . gen long id = _n
        reshape long dx_, i(id) j(dx_ecode_id)
        replace dx_ecode_id = 2 if dx_ecode_id > 2
        drop if missing(dx_)
        //drop id

    // Collapse
        keep iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_ecode_id metric_* dx_
        collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_ecode_id dx_) fast

        //rename dx_ dx_mapped_

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
		replace age_end = -99 if age_start == -99

    // STANDARDIZE SEX
        //drop if sex != 1 & sex != 2

    // LOCATION_ID
        // done already

    // COLLAPSE CASES
        // fastcollapse cases deaths, type(sum) by(iso3 subdiv location_id source year age_* sex platform icd_vers cause_primary)
        // fastcollapse discharges deaths, type(sum) by(iso3 subdiv location_id source national year age_* sex platform icd_vers dx_ dx_ecode_id NID)
        collapse (sum) cases deaths, by(iso3 subdiv location_id source national year age_* sex platform icd_vers dx_ dx_ecode_id NID) fast

    // modify columns to fit our structure
        rename dx_ cause_code
        rename dx_ecode_id diagnosis_id


	// SAVE
		compress
		save "`output_folder'/formatted_USA_NHDS_79_10.dta", replace
		save "`archive_folder'/formatted_USA_NHDS_79_10_`today'.dta", replace

	capture log close


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
