** **************************************************************************
// Purpose: Reassign deaths that violate GBD cause-based age-sex restrictions into garbage categories
** **************************************************************************
** CONFIGURATION
** **************************************************************************
	** ****************************************************************
	** Prepare STATA for use
	**
	** This section sets the application perferences.  The local applications
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
					global prefix "/home/j"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "J:"
				}

			// Set up PDF maker
				do "$prefix/Usable/Tools/ADO/pdfmaker_Acrobat10.do"

			// Set up DB string
			run "$prefix/WORK/10_gbd/00_library/functions/create_connection_string.ado"
			create_connection_string
			local conn_string = r(conn_string)
	** ****************************************************************
	** DEFINE LOCALS
	** ****************************************************************
		// RESTRICTION FILE
			local restriction_file "$prefix/WORK/00_dimensions/03_causes/gbd2015_causes_all.dta"

		// SET DATE
			local today "`3'"

		// SET USERNAME
			local username "`1'"

		// SET SOURCE
			local source "`2'"

		// SET CAUSE TYPE (for causes of death, this should ALWAYS be yll)
			local cause_type "yll"

	** ****************************************************************
	** CREATE LOG
	** ****************************************************************
		local log_folder "$prefix/WORK/03_cod/01_database/03_datasets/`source'/logs"
		capture mkdir "`log_folder'"
		capture log close
		log using "`log_folder'/03_restrictions_`source'_`today'", replace

** **************************************************************************
** RUN PROGRAGM
** **************************************************************************
	// Get restriction file
		odbc load, exec("SELECT acause, male, female, `cause_type'_age_start, `cause_type'_age_end FROM shared.cause_hierarchy_history WHERE cause_set_version_id IN (SELECT max(cause_set_version_id) FROM shared.cause_set_version WHERE cause_set_id=4)") `conn_string' clear
		rename acause yll_cause
		tempfile restriction_data
		save `restriction_data', replace

	// Get source data
		use "$prefix/WORK/03_cod/01_database/03_datasets/`source'/data/intermediate/02_agesexsplit.dta", clear		
	// Make new terminal age group (80+)
		egen double tmp = rowtotal(deaths22 deaths23)
		replace deaths22 = tmp
		replace deaths23 = 0
		drop tmp
		rename acause yll_cause
		merge m:1 yll_cause using `restriction_data', keep(1 3) nogen
	// 
		foreach sex in male female {
			replace `sex' = 1 if `sex' == .
		}
		replace `cause_type'_age_start = 0 if `cause_type'_age_start == .
		replace `cause_type'_age_end = 80 if `cause_type'_age_end == .
		replace `cause_type'_age_end = 95 if `cause_type'_age_end == 80
	// Drop irrelevant ages
		foreach a of numlist 1/2 4/6 26 {
			capture drop deaths`a'
		}
	// Generate code which will receive restricted deaths
		/*
	NOTE: this will be code ZZZ except: 
		- cancers which will go to the all cancer cause 
		- IBD and vascular intestinal disorders are restricted under 1, should go to diarrhea in infants
		replace restricted_cause = "acause_diarrhea" if inlist(acause,"digest_ibd","digest_vascular")
	*/
		gen restricted_cause = "ZZZ"
		levelsof(list), local(code_version) clean
		if "`code_version'" == "ICD10" {
			** Cancers
			replace restricted_cause = "B999" if substr(cause,1,1) == "A" | substr(cause,1,1) == "B"
			replace restricted_cause = "D499" if substr(cause,1,1) == "C" | substr(cause,1,1) == "D"
			replace restricted_cause = "I999" if substr(cause,1,1) == "I"
			replace restricted_cause = "J989" if substr(cause,1,1) == "J"
			replace restricted_cause = "K929" if substr(cause,1,1) == "K"
			replace restricted_cause = "Y89" if substr(cause,1,1) == "V" | substr(cause,1,1) == "Y"
			
			** IBD and vascular intestinal disorders
			replace restricted_cause = "acause_diarrhea" if inlist(yll_cause,"digest_ibd","digest_vascular")
		}
		if "`code_version'" == "ICD9_detail" {
			** Cancers
			gen numeric_cause = real(cause)
			replace restricted_cause = "1398" if numeric_cause >= 001 & numeric_cause < 140
			replace restricted_cause = "2399" if numeric_cause >= 140 & numeric_cause < 240
			replace restricted_cause = "4599" if numeric_cause >= 390 & numeric_cause < 460
			replace restricted_cause = "5199" if numeric_cause >= 460 & numeric_cause < 520
			replace restricted_cause = "578" if numeric_cause >= 520 & numeric_cause < 580
			replace restricted_cause = "E989" if substr(cause,1,1) == "E"
			drop numeric_cause
		}
		
	// Cycle through ages and sexes to see if there are violations
		foreach age of numlist 3 7/25 91/94 {
			// Replace blanks with 0s
				display in red "Replace blanks in deaths`age'"
				replace deaths`age' = 0 if deaths`age' == .
			// Generate restriction variable for age group
				gen double restricted`age' = 0
			// Assign age value for each age group
				if `age' == 3 local age_val 1
				if `age' == 91 local age_val 0
				if `age' == 92 local age_val 0.001
				if `age' == 93 local age_val 0.01
				if `age' == 94 local age_val 0.1
				forvalues a = 7(1)25 {
					if `age' == `a' local age_val = (`a' - 6) * 5
				}
			// Place restricted deaths into restriced category & remove from deaths category
				display in red "Checking deaths`age' (age `age_val')"
				replace restricted`age' = deaths`age' if (sex == 1 & male == 0) | (sex == 2 & female == 0) | (`age_val' < `cause_type'_age_start) | (`age_val' > `cause_type'_age_end)
				display in red "Moving restricted deaths in deaths`age' (age `age_val')"
				replace deaths`age' = deaths`age' - restricted`age'
		}
	// Remove restriction parameters
		drop male female `cause_type'_age_*
	// Separate and append on restricted data
		preserve
			drop cause deaths*
			foreach age of numlist 3 7/25 91/94 {
				rename restricted`age' deaths`age'
			}
			rename restricted_cause cause
			** ******************************************
			** Standard remap is to garbage
			replace yll_cause = "_gc"
			replace cause_name = ""

			** Change remap of acause_* causes to their assignments
			replace yll_cause = subinstr(cause,"acause_","",1) if index(cause,"acause_")
			** ******************************************
			collapse (sum) deaths*, by(iso3 subdiv national source source_label source_type NID list frmat im_frmat sex year cause cause_name yll_cause dev_status region location_id) fast
			tempfile restricted_data
			save `restricted_data', replace
		restore
		drop restricted*
		append using `restricted_data', force
	// Collapse
		collapse (sum) deaths*, by(iso3 subdiv national source source_label source_type NID list frmat im_frmat sex year cause cause_name yll_cause dev_status region location_id) fast
	// Fill in all deaths variables
		foreach age of numlist 3/26 91/94 {
			capture gen deaths`age' = 0
		}
		egen double deaths1 = rowtotal(deaths*)
		gen double deaths2 = deaths91 + deaths92 + deaths93 + deaths94
	// Drop if deaths1 is 0
		drop if deaths1 == 0
	// Order variables
		#delimit ;
		order
		iso3 subdiv national region dev_status
		source source_label source_type NID list
		frmat im_frmat
		sex year cause cause_name
		deaths1 deaths2 deaths3 deaths4 deaths5 deaths6 deaths7 deaths8 deaths9 deaths10 deaths11 deaths12 deaths13 deaths14 deaths15
		deaths16 deaths17 deaths18 deaths19 deaths20 deaths21 deaths22 deaths23 deaths24 deaths25 deaths26 deaths91 deaths92 deaths93 deaths94 ;
	
		#delimit cr
	// Save
		compress
		rename yll_cause acause
		save "$prefix/WORK/03_cod/01_database/03_datasets/`source'/data/intermediate/03_corrected_restrictions.dta", replace
		save "$prefix/WORK/03_cod/01_database/03_datasets/`source'/data/intermediate/_archive/03_corrected_restrictions_`today'.dta", replace
		
	capture log close


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
