// USERNAME
// HOSPITAL EPI COMPILE CLUSTER: set up 5 year bands



	// Set application preferences
		// Clear memory and set memory and variable limits
			clear all
			set maxvar 32000

		// Set to run all selected code without pausing
			set more off

		// Define J drive (data) for cluster (UNIX) and Windows (Windows)
			if c(os) == "Unix" {
				global prefix "FILEPATH"
				set odbcmgr unixodbc
			}
			else if c(os) == "Windows" {
				global prefix "FILEPATH"
			}

		// Program Arguments
			local source `1'
			// local source "USA_HCUP_SID"

		// ODBC Settings
			// Database dsn
			local dsn "prodcod"
			// location_set_version_id - set to latest GBD Computing
			local location_version = 25

		// Temp Folder
		// FILEPATH
			// local temp_folder "FILEPATH"
			local temp_folder "FILEPATH" // FILEPATH

		// IHME written mata functions which speed up collapse and egen
			do "FILEPATH"

***********************************************************************************
***********************************************************************************
***********************************************************************************
	if "`source'" == "SWE_PATIENT_REGISTRY_98_12" | "`source'" ==  "NOR_NIPH_08_12" | "`source'" ==  "EUR_HMDB" | "`source'" ==  "ECU_INEC_97_11" {
		use "FILEPATH", clear
		gen dx_ecode_id = 1
		rename cause_primary dx_mapped_
	}
	else if "`source'" == "USA_HCUP_SID" {
		clear
		foreach i in 03 04 05 06 07 08 09 {
			append using "FILEPATH"
		}
		replace source = "`source'"
	}
	else if "`source'" == "MISSING_LOCATIONS" {
		use "FILEPATH", clear
		keep if subdiv == "AZ" | subdiv == "AR"
		replace location_id = 525 if subdiv == "AZ"
		replace location_id = 526 if subdiv == "AR"
		replace metric_discharges_weighted = metric_discharges
		replace metric_deaths_weighted = metric_deaths
		replace source = "USA_HCUP_SID"
	}
	// deprecated to remove day cases from nzl
	// This is to prep just the new data from 2015
	//else if "`source'" == "NZL_NMDS" {
	//	use "FILEPATH", clear
		// add the correct NID for 2015
	//	replace NID = 293984
	//}
	else {
		use "FILEPATH", clear
	}



	if "`source'" == "EUR_HMDB" {
		drop if inlist(iso3, "AUT", "SWE", "NOR", "GBR")
	}


	// STANDARDIZE PLATFORMS
		replace platform = "Outpatient" if platform == "Emergency"
		expand 2 if platform == "Inpatient + Outpatient", gen(new)
		replace platform = "1" if platform == "Inpatient + Outpatient" & new == 0
		replace platform = "2" if platform == "Inpatient + Outpatient" & new == 1
		drop new
		replace platform = "1" if platform == "Inpatient" | platform == "Inpatient 1" | platform == "Inpatient 2"
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
		if "`source'" == "CAN_DAD_94_09" | "`source'" == "CAN_NACRS_02_09" {
			replace age = 0 if age < 5
		}
		rename age age_start
		gen age_end = age_start + 4
		replace age_end = 1 if age_start == 0
		replace age_end = 99 if age_start == 95
		replace age_end = 4 if age_start == 1
		if "`source'" == "CAN_DAD_94_09" | "`source'" == "CAN_NACRS_02_09" {
			replace age_end = 4 if age_start == 0
		}
		if "`source'" == "EUR_HMDB" {
			replace age_end = 14 if age_start == 0 & frmat == 112
			replace age_end = 64 if age_start == 15 & frmat == 112
			replace age_end = 99 if age_start == 65 & frmat == 112
		}

	// STANDARDIZE SEX
		drop if sex != 1 & sex != 2

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
		fastcollapse cases deaths, type(sum) by(iso3 subdiv location_id source national year age_* sex platform icd_vers dx_mapped_ dx_ecode_id NID)

	save "FILEPATH", replace
