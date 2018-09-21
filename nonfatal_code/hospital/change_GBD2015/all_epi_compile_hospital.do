// USERNAME
// Dec 2015
// Purpose: Temporary script to convert 02_selected_primary from each source to hospital data for epi extractions
//		Drops sample_size & other extraneous variables, aggregates metrics, and aggregates to GBD years.

	clear
	set more off

	if c(os) == "Windows" {
		global prefix "J:"
	}
	else {
		global prefix "FILENAME"
		set odbcmgr unixodbc
	}

	// local temp_folder "FILENAME"
	local temp_folder "FILENAME"

	// quietly {
	// 	** ****************************************************************
	// 	** CREATE LOG
	// 	** ****************************************************************
	// 		local log_folder "FILENAME"
	// 		capture mkdir "`log_folder'"
	// 		capture log close
	// 		if "`sources'" == "" {
	// 			log using "FILENAME", replace
	// 		}
	// 		else {
	// 			local source_names = subinstr("`sources'"," ","_",.)
	// 			log using "FILENAME", replace
	// 		}

	// Location IDs
	// we just made a dta table manually outside of this
	// get ids call get_location_metadata
		// local dsn prodcod
		// local location_version 25
		// odbc load, exec(SQL QUERY) dsn(`dsn') clear
		// keep location_id local_id
		// duplicates drop
		// rename local_id iso3
		// compress
		// save "FILENAME", replace


		//only primary: 	SWE_PATIENT_REGISTRY_98_12 NOR_NIPH_08_12 EUR_HMDB ECU_INEC_97_11
		//secondary 		AUT_HDD BRA_SIA BRA_SIH CAN_DAD_94_09 CAN_NACRS_02_09 MEX_SINAIS NZL_NMDS USA_HCUP_SID USA_NAMCS USA_NHAMCS_92_10 USA_NHDS_79_10

	local sources "AUT_HDD BRA_SIA BRA_SIH CAN_DAD_94_09 CAN_NACRS_02_09  ECU_INEC_97_11 EUR_HMDB MEX_SINAIS NOR_NIPH_08_12 NZL_NMDS SWE_PATIENT_REGISTRY_98_12 USA_HCUP_SID USA_NAMCS USA_NHAMCS_92_10 USA_NHDS_79_10"

	// Temporarily removed AUT_HDD USA_HCUP_SID, and NOR_NIPH_08_12 cuz we ran those individually and we'll append later
	// local sources "BRA_SIA BRA_SIH CAN_DAD_94_09 CAN_NACRS_02_09  ECU_INEC_97_11 EUR_HMDB MEX_SINAIS NOR_NIPH_08_12 NZL_NMDS SWE_PATIENT_REGISTRY_98_12 USA_NAMCS USA_NHAMCS_92_10 USA_NHDS_79_10"


	foreach source of local sources {
		//cap mkdir "FILENAME"
		//cap rm "FILENAME"
		//cap !rm "FILENAME"

		if "`source'" == "USA_HCUP_SID" {
			cap mkdir "`temp_folder'/`source'"
			cap rm "`temp_folder'/`source'/`source'_prepped.dta"
			cap !rm "`temp_folder'/`source'/`source'_prepped.dta"
			!qsub -P ihme_general -pe multi_slot 30 -l mem_free=60g -N "FILENAME" "FILENAME" "`source'"
		}
		else {
			cap mkdir "FILENAME"
			cap rm "FILENAME"
			cap !rm "FILENAME"
			!qsub -P ihme_general -pe multi_slot 12 -l mem_free=24g -N "`source'" "FILENAME "FILENAME "`source'"
			// !qsub -P proj_hospital -pe multi_slot 6 -l mem_free=16g -N "`source'" "FILENAME "FILENAME "`source'"
		}
	}

	sleep 300000
	clear

	foreach source of local sources {
		capture confirm file "FILENAME"
		if _rc == 0 {
			display "FOUND `source'!"
		}
		while _rc == 601 {
			display "`source' not found, checking again in 15 seconds"
			sleep 60000
			capture confirm file "FILENAME"
			if _rc == 0 {
				display "FOUND!"
			}
		}
		sleep 10000
		display "...APPENDING `source'"
		append using "FILENAME"
		compress
	}

	// give resulting file a new name. save to temp?
	compress
	save "FILENAME", replace
