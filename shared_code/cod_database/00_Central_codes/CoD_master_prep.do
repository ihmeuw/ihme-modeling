** ****************************************************
** Purpose: Prepare and compile all final Cause of Death sources for COD Viz
** ****************************************************

quietly {

// Clear the mind
	clear all
	clear mata
	pause off
	set more off

// Set the time
	local date = c(current_date)
	local today = date("`date'", "DMY")
	local year = year(`today')
	local month = month(`today')
	local day = day(`today')
	local time = c(current_time)
	local time : subinstr local time ":" "", all
	local length : length local month
	if `length' == 1 local month = "0`month'"	
	local length : length local day
	if `length' == 1 local day = "0`day'"
	global date = "`year'_`month'_`day'"
	global timestamp = "${date}_`time'"

// Establish directories
	// J:drive
	if c(os) == "Windows" {
		global j "J:"
	}
	if c(os) == "Unix" {
		global j "/home/j"
		set odbcmgr unixodbc
	}

	run "$j/WORK/10_gbd/00_library/functions/create_connection_string.ado"
	create_connection_string
	local conn_string = r(conn_string)

	// Temp directory for intermediate steps within each program
	capture mkdir "/ihme/cod/prep"
	capture mkdir "/ihme/cod/prep/01_database/"
	global temp_dir "/ihme/cod/prep/01_database/"

	// Working directories
	global source_dir "$j/WORK/03_cod/01_database/03_datasets/"
	global source_prog_dir "/homes/$username/cod-data/03_datasets/"
	global prog_dir "/homes/$username/cod-data/02_programs"
	global log_dir "/ihme/cod/prep/01_database/02_programs/prep/logs"
	global database_dir "/homes/$username/cod-data/02_programs/compile"
	global cause_dir "$j/WORK/00_dimensions/03_causes"
	global package_dir "/ihme/cod/prep/01_database/02_programs/redistribution/rdp/"

// Set the globals for the final steps: merging through upload if the operator wants all of them run
	if $step_07_to_13 == 1 {
		// Merge 
		global step_07_mergelong		= 1
		// Age-and-sex-specific cause recode
		global step_08_recode			= 1
		// Source-type-specific adjustments
		global step_09_adjust			= 1
		// Cause aggregation
		global step_10_aggregate		= 1
		// Noise reduction algorithms
		global step_11_nra				= 1
		// Clean all variables for upload
		global step_12_clean			= 1
		// Upload
		global step_13_upload			= 1
	}

// Clear out any old envelope data
	
	odbc load, exec("SELECT output_version_id FROM mortality.output_version WHERE is_best=1") `conn_string' clear
	local ovid = output_version_id in 1
	capture confirm file "/ihme/cod/prep/01_database/mortality_envelope_v`ovid'.dta"
	if _rc {
		noisily display "Prepping envelope `ovid'..."
		do "~/cod-data/01_database/02_programs/prep/code/env_master.do"
		noisily display "... saved (/ihme/cod/prep/01_database/mortality_envelope.dta)"
	} 
	else {
		noisily display "Using envelope `ovid'"
	}
	
// Add to the record of COD Preps
	import excel using "$j/WORK/03_cod/00_documentation/COD_PREP_RECORD_GBD2015.xls", firstrow clear
	count
	local new = `r(N)' + 1
	set obs `new'
	replace run_date = "$date" in `new'
	replace run_time = "`time'" in `new'
	if "$source" == "" {
		replace sources = "ALL" in `new'
		}
	else if "$source" != "" {
		replace sources = "$source" in `new'
	}
	replace format = "X" in `new' if $step_00_format==1
	replace map = "X" in `new' if $step_01_map==1
	replace before_agesex = "X" in `new' if $step_02a_before_agesex ==1
	replace agesexsplit = "X" in `new' if $step_02_agesexsplit==1
	replace restriction = "X" in `new' if $step_03_agesexrestrict==1
	replace before_rd = "X" in `new' if $step_04_before_rdp==1
	replace redistribute = "X" in `new' if $step_05_rdp==1
	replace hiv = "X" in `new' if $step_06_hiv==1
	replace merge = "X" in `new' if $step_07_mergelong==1
	replace recode = "X" in `new' if $step_08_recode==1
	replace adjust = "X" in `new' if $step_09_adjust==1
	replace aggregate = "X" in `new' if $step_10_aggregate==1
	replace NRA = "X" in `new' if $step_11_nra==1
	replace clean = "X" in `new' if $step_12_clean==1
	replace upload = "DATABASE COMPILED" in `new' if $step_13_upload==1
	replace user = "$username" in `new'
	replace notes = "$notes" in `new'
	sort run_date run_time
	export excel using "$j/WORK/03_cod/00_documentation/COD_PREP_RECORD_GBD2015.xls", firstrow(variables) replace
	clear

// Log our efforts
	capture log close
	if "$source" == "" | length("$source")>35 {
	log using "$log_dir/prep_ALL_${timestamp}", replace	
	}

	else if "$source" != "" & length("$source")<=35 {
	log using "$log_dir/prep_${source}_${timestamp}", replace
	}

// Set up the list of sources to run
	if "$source" != "" {
		local sources "$source"
		local sources: list clean sources
		local n = 1
		set obs 1
		gen source=""
		foreach source of local sources {
			replace source = "`source'" in `n'
			local n = `n' + 1
			set obs `n'
		}
		drop in `n'
		levelsof source, local(sources) clean
		}

	** for all sources, we ignore the test sources starting with "_"
	if "$source" == "" {
		local sources: dir "$source_dir" dirs "*", respectcase
		local sources: list clean sources
		local n = 1
		set obs 1
		gen source=""
		foreach source of local sources {
		replace source = "`source'" in `n'
		local n = `n' + 1		
		set obs `n'
		}
		drop in `n'
		drop if substr(source, 1, 1)=="_"
		levelsof source, local(sources) clean
		}

// If we're remapping, re stat-transfer the maps
noisily display "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
noisily display "+++++++++++++++++++++| TRANSFERRING MAPS |++++++++++++++++++++++++"
	if $step_01_map == 1 | $step_05_rdp == 1 {
		local imported_icd10 = 0
		local imported_icd9 = 0
		local imported_10tab = 0
		local imported_icd10va = 0
		foreach source of local sources {
			** ICD10
			local icd10_source_count = 0
			foreach icd_source of global icd10sources {
				if "`source'" == "`icd_source'"	local icd10_source_count = 1
			}
			if `icd10_source_count' > 0 & `imported_icd10' == 0 {
				global cod_source "ICD10"
				noisily display "Importing $cod_source map"
				!python "$j/WORK/00_dimensions/03_causes/master_cause_map.py" "$cod_source"
				copy "$cause_dir/temp/map_ICD10.dta" "$source_dir/ICD10/maps/cause_list/map_ICD10.dta", replace
				local imported_icd10 = 1
			}
			foreach icd_source of global icd10sources {
				if "`source'" == "`icd_source'"	copy "$source_dir/ICD10/maps/cause_list/map_ICD10.dta" "$source_dir//`icd_source'//maps/cause_list/map_`icd_source'.dta", replace
			}
			** ICD9_detail
			local icd9_source_count = 0
			foreach icd_source of global icd9sources {
				if "`source'" == "`icd_source'"	local icd9_source_count = 1
			}
			if `icd9_source_count' > 0 & `imported_icd9' == 0 {
				global cod_source "ICD9_detail"
				noisily display "Importing $cod_source map"
				!python "$j/WORK/00_dimensions/03_causes/master_cause_map.py" "$cod_source"
				copy "$cause_dir/temp/map_ICD9_detail.dta" "$source_dir/ICD9_detail/maps/cause_list/map_ICD9_detail.dta", replace
				local imported_icd9 = 1
			}
			foreach icd_source of global icd9sources {
				if "`source'" == "`icd_source'" copy "$source_dir/ICD9_detail/maps/cause_list/map_ICD9_detail.dta" "$source_dir//`icd_source'//maps/cause_list/map_`icd_source'.dta", replace
			}
			** ICD10_tabulated
			local icd10tab_source_count = 0
			foreach icd_source of global icd10tabsources {
				if "`source'" == "`icd_source'"	local icd10tab_source_count = 1
			}
			if `icd10tab_source_count' > 0 & `imported_10tab' == 0 {
				global cod_source "ICD10_tabulated"
				noisily display "Importing $cod_source map"
				!python "$j/WORK/00_dimensions/03_causes/master_cause_map.py" "$cod_source"
				copy "$cause_dir/temp/map_ICD10_tabulated.dta" "$source_dir/ICD10_tabulated/maps/cause_list/map_ICD10_tabulated.dta", replace
				local imported_10tab = 1
			}
			foreach icd_source of global icd10tabsources {
				if "`source'" == "`icd_source'" copy "$source_dir/ICD10_tabulated/maps/cause_list/map_ICD10_tabulated.dta" "$source_dir//`icd_source'//maps/cause_list/map_`icd_source'.dta", replace
			}
			** ICD10_VA (using INDEPTH dataset)
			local icd10va_source_count = 0
			foreach icd_source of global icd10vasources {
				if "`source'" == "`icd_source'"	local icd10va_source_count = 1
				if "`source'" == "`icd_source'" copy "$source_dir/INDEPTH_ICD10_VA/maps/cause_list/map_INDEPTH_ICD10_VA.dta" "$source_dir//`icd_source'//maps/cause_list/map_`icd_source'.dta", replace
			}
			if `icd10va_source_count' > 0 & `imported_icd10va' == 0 {
				global cod_source "INDEPTH_ICD10_VA"
				noisily display "Importing $cod_source map"
				!python "$j/WORK/00_dimensions/03_causes/master_cause_map.py" "$cod_source"
				copy "$cause_dir/temp/map_INDEPTH_ICD10_VA.dta" "$source_dir/INDEPTH_ICD10_VA/maps/cause_list/map_INDEPTH_ICD10_VA.dta", replace
				local imported_icd10va = 1
			}
			foreach icd_source of global icd10vasources {
				if "`source'" == "`icd_source'" copy "$source_dir/INDEPTH_ICD10_VA/maps/cause_list/map_INDEPTH_ICD10_VA.dta" "$source_dir//`icd_source'//maps/cause_list/map_`icd_source'.dta", replace
			}
			if `icd9_source_count' == 0 & `icd10_source_count' == 0 & `icd10tab_source_count' == 0 & `icd10va_source_count' == 0 {
				global cod_source = "`source'"
				noisily display "Importing $cod_source map"
				capture log close
				!python "$j/WORK/00_dimensions/03_causes/master_cause_map.py" "$cod_source"
				copy "$cause_dir/temp/map_`source'.dta" "$source_dir/`source'/maps/cause_list/map_`source'.dta", replace
				if "`source'" == "ICD10" local imported_icd10 = 1
				if "`source'" == "ICD9_detail" local imported_icd9 = 1
				if "`source'" == "ICD10_tabulated" local imported_10tab = 1
				if "`source'" == "INDEPTH_ICD10_VA" local imported_10tab = 1
			}
		}
	}

// If we're running redistribution, regenerate the packages
noisily display "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
noisily display "+++++++++++++++++++++| TRANSFERRING RDPS |++++++++++++++++++++++++"
	if $step_05_rdp == 1 {
		foreach source of local sources {
			** ICD10
			local icd_source_count = 0
			foreach icd_source of global icd10sources {
				if "`source'" == "`icd_source'" local icd_source_count = 1
				if "`source'" == "`icd_source'" local cod_source "ICD10"
			}
			** ICD9_detail
			foreach icd_source of global icd9sources {
				if "`source'" == "`icd_source'" local icd_source_count = 1
				if "`source'" == "`icd_source'" local cod_source "ICD9_detail"
			}
			** ICD10_tabulated
			foreach icd_source of global icd10tabsources {
				if "`source'" == "`icd_source'" local icd_source_count = 1
				if "`source'" == "`icd_source'" local cod_source "ICD10_tabulated"
			}
			** INDEPTH_ICD10_VA
			foreach icd_source of global icd10vasources {
				if "`source'" == "`icd_source'" local icd_source_count = 1
				if "`source'" == "`icd_source'" local cod_source "INDEPTH_ICD10_VA"
			}
			** Everything else
			if `icd_source_count' == 0 {
				local cod_source "`source'"
			}
			** Now submit jobs
			if `icd_source_count' == 0 & !inlist("`source'", "ICD10", "ICD9_detail", "ICD10_tabulated", "INDEPTH_ICD10_VA", "ICD9_BTL","Russia_FMD_1989_1998", "Russia_FMD_1999_2011","Russia_FMD_2012_2013", "ICD8_detail") {
				noisily display "Beginning package generation for `cod_source'"
				use "$j/WORK/00_dimensions/03_causes/temp/packagesets_`cod_source'.dta", clear
				destring(package_set_id), replace
				destring(code_system_id), replace
				levelsof(package_set_id), local(package_sets) clean
				foreach package_set_id of local package_sets {
					levelsof(code_system_id) if package_set_id == `package_set_id', local(code_system_id) clean
					capture rm "$package_dir/`package_set_id'/cause_map.csv"
					if "`source'" != "VA_lit_GBD_2010_2013" {
						!qsub -P proj_codprep -pe multi_slot 2 -l mem_free=4g -N "CoD_05_`cod_source'_`package_set_id'_packages" "$prog_dir/prep/code/shellpython.sh" "$prog_dir/redistribution/code/make_packages.py" "`code_system_id' `package_set_id'"
					}
				}
				if "`source'" == "VA_lit_GBD_2010_2013" {
					!qsub -P proj_codprep -pe multi_slot 2 -l mem_free=4g -N "CoD_05_`cod_source'_`package_set_id'_packages" "$prog_dir/prep/code/shellpython.sh" "$prog_dir/redistribution/code/make_packages_va.py"
				}
			}
		}
	}

// Erase all the completed step files for the steps being run
// This breaks preceding steps if the previous step was not run successfully
	foreach source of local sources {
		if $step_00_format == 1 {
			capture erase "$source_dir//`source'//data/intermediate/00_formatted.dta"
		}
		if $step_01_map == 1 {
			capture erase "$source_dir//`source'//data/intermediate/01_mapped.dta"
		}
		if $step_02a_before_agesex==1 {
			capture erase "$source_dir//`source'//data/intermediate/02a_before_agesex.dta"
		}
		if $step_02_agesexsplit == 1 {
			capture erase "$source_dir//`source'//data/intermediate/02_agesexsplit.dta"
			capture erase "$source_dir//`source'//data/intermediate/02_agesexsplit_compile.dta"
		}
		if $step_03_agesexrestrict == 1 {
			capture erase "$source_dir//`source'//data/intermediate/03_corrected_restrictions.dta"
			capture erase "$source_dir//`source'//data/intermediate/03_corrected_restrictions_compile.dta"
		}
		if $step_04_before_rdp == 1 {
			capture erase "$source_dir//`source'//data/intermediate/04_before_redistribution.dta"
		}
		if $step_05_rdp == 1 {
			capture erase "$source_dir//`source'//data/final/05a_after_redistribution.dta"
			capture erase "$source_dir//`source'//data/final/05b_for_compilation.dta"
			capture erase "$source_dir//`source'//data/final/`source'_redistributed.csv"
		}
		if $step_06_hiv == 1 {
			capture erase "$source_dir//`source'//data/final/06_hiv_corrected.dta"
		}
		if $step_07_mergelong == 1 {
			capture erase "$source_dir//`source'//data/final/07_merged_long.dta"
		}
		if $step_08_recode == 1 {
			capture erase "$source_dir//`source'//data/final/08_recoded.dta"
		}
		if $step_09_adjust == 1 {
			capture erase "$source_dir//`source'//data/final/09_adjusted.dta"
		}
		if $step_10_aggregate == 1 {
			capture erase "$source_dir//`source'//data/final/10_aggregated.dta"
		}
		if $step_11_nra == 1 {
			capture erase "$source_dir//`source'//data/final/11_noise_reduced.dta"
		}
		if $step_12_clean == 1 {
			capture erase "$source_dir//`source'//data/final/12_cleaned.dta"
		}
		if $step_13_upload==1 {
			capture erase "/ihme/cod/prep/01_database/13_upload/data/signal_`source'_complete.dta"
		}
	}
	
// Lastly, if this is some guest user set username to blank so it will use the correct shell script
	if inlist("$username", "strUser", "strUser", "strUser", "strUser", "strUser", "strUser", "strUser", "strUser") !=1 {
		global username "guest"
	}

// Let the user know that we're about to submit the jobs
noisily display "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
noisily display "+++++++++++++++++++++| SUBMITTING PREP JOBS |+++++++++++++++++++++"

** *************************************************
** Step 00_format: Run the code which formats all the raw extractions into CoD useable files. This is seldom necessary.
** *************************************************

if $step_00_format == 1 {
	foreach source of local sources {
		!qsub -P proj_codprep -pe multi_slot 8 -l mem_free=10g -N "CoD_00_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$source_prog_dir//`source'//code/format_`source'.do" "$timestamp"
	}
}

** *************************************************
** Step 01_map: Run the code that merges on the cause map and geographic identifiers
** *************************************************

if $step_01_map == 1 {
	if $step_00_format == 0 {
		foreach source of local sources {
			!qsub -P proj_codprep -pe multi_slot 6 -l mem_free=10g -N "CoD_01_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$prog_dir/mapping/code/map_sources.do" "`source' $timestamp"
		}
	}
	else if $step_00_format == 1 {
		foreach source of local sources {
			!qsub -P proj_codprep -pe multi_slot 6 -l mem_free=10g -N "CoD_01_`source'" -hold_jid "CoD_00_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$prog_dir/mapping/code/map_sources.do" "`source' $timestamp"
		}
	}
}


** *************************************************
** Step 02a_before_agesex: Urban-rural splitting. Skips for all datasets besides India_CRS
** *************************************************

if $step_02a_before_agesex == 1 {
	if $step_01_map == 0 {
		foreach source of local sources {
			!qsub -P proj_codprep -pe multi_slot 10 -l mem_free=20g -N "CoD_02a_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$prog_dir/before_agesex/code/before_agesex.do" "$username `source' $timestamp $temp_dir $source_dir"
		}
	}
	else if $step_01_map == 1 {
		foreach source of local sources {
			!qsub -P proj_codprep -pe multi_slot 10 -l mem_free=20g -N "CoD_02a_`source'" -hold_jid "CoD_01_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$prog_dir/before_agesex/code/before_agesex.do" "$username `source' $timestamp $temp_dir $source_dir"
		}
	}
}

** *************************************************
** Step 02_agesexsplit: Age-sex split each dataset. This skips datasets that don't need it.
** *************************************************

if $step_02_agesexsplit == 1 {
	if $step_02a_before_agesex == 0 {
		foreach source of local sources {
			!qsub -P proj_codprep -pe multi_slot 10 -l mem_free=20g -N "CoD_02_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$prog_dir/age_sex_splitting/code/split_agesex.do" "$username `source' $timestamp $temp_dir $source_dir"
		}
	}
	else if $step_02a_before_agesex == 1 {
		foreach source of local sources {
			!qsub -P proj_codprep -pe multi_slot 10 -l mem_free=20g -N "CoD_02_`source'" -hold_jid "CoD_02a_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$prog_dir/age_sex_splitting/code/split_agesex.do" "$username `source' $timestamp $temp_dir $source_dir"
		}
	}
}

** *************************************************
** 	Step 03_agesexrestrict: Correct age-sex violations by moving them to garbage (ZZZ) or all neoplasm, if the cause was cancer
** *************************************************

if $step_03_agesexrestrict == 1 {
	if $step_02_agesexsplit == 0 {
		foreach source of local sources {
			!qsub -P proj_codprep -pe multi_slot 6 -l mem_free=10g -N "CoD_03_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$prog_dir/restriction/code/correct_restriction_violations.do" "$username `source' $timestamp"
		}
	}
	else if $step_02_agesexsplit == 1 {
		foreach source of local sources {
			!qsub -P proj_codprep -pe multi_slot 6 -l mem_free=10g -N "CoD_03_`source'" -hold_jid "CoD_02_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$prog_dir/restriction/code/correct_restriction_violations.do" "$username `source' $timestamp"
		}
	}
}

** *************************************************
** Step 04_before_rdp: Some datasets need special treatment and adjustments at this point
** *************************************************

if $step_04_before_rdp == 1 {
	local snum = 0
	foreach source of local sources {
		local ++snum
		** DUSERt to source
		local `snum'_map "`source'"
		** ICD10
		foreach icd_source of global icd10sources {
			if "`source'" == "`icd_source'" local `snum'_map "ICD10"
		}
		** ICD9_detail
		foreach icd_source of global icd9sources {
			if "`source'" == "`icd_source'" local `snum'_map "ICD9_detail"
		}
	}
	local snum = 0
	if $step_03_agesexrestrict == 0 {
		foreach source of local sources {
		local ++snum
			!qsub -P proj_codprep -pe multi_slot 6 -l mem_free=12g -N "CoD_04_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$prog_dir/pre_rdp/fix_before_redistribution.do" "$username `source' $timestamp ``snum'_map'"
		}
	}
	else if $step_03_agesexrestrict == 1 {
		foreach source of local sources {
		local ++snum
			!qsub -P proj_codprep -pe multi_slot 6 -l mem_free=12g -N "CoD_04_`source'" -hold_jid "CoD_03_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$prog_dir/pre_rdp/fix_before_redistribution.do" "$username `source' $timestamp ``snum'_map'"
		}
	}
}

** *************************************************
** Step 05_rdp: Redistribute garbage codes using Python redistribution
** *************************************************

	if $step_05_rdp == 1 {
		if $step_04_before_rdp == 0 {
			foreach source of local sources {
			** Set dUSERt code_version as name of data source
			local code_version "`source'"
			** ICD10
			foreach icd_source of global icd10sources {
				if "`source'" == "`icd_source'"	local code_version "ICD10"
			}
			** ICD9_detail
			foreach icd_source of global icd9sources {
				if "`source'" == "`icd_source'"	local code_version "ICD9_detail"
			}
			** ICD10_tabulated
			foreach icd_source of global icd10tabsources {
				if "`source'" == "`icd_source'"	local code_version "ICD10_tabulated"
			}
			** ICD10_VA
			foreach icd_source of global icd10vasources {
				if "`source'" == "`icd_source'"	local code_version "INDEPTH_ICD10_VA"
			}
			** Submit job
				!qsub -P proj_codprep -pe multi_slot 5 -l mem_free=10g -N "CoD_05_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$prog_dir/redistribution/code/cod_rdp_master.do" "$username `source' `code_version' $timestamp"
			}
		}
		else if $step_04_before_rdp == 1 {
			foreach source of local sources {
			** Set dUSERt code_version as name of data source
			local code_version "`source'"
			** ICD10
			foreach icd_source of global icd10sources {
				if "`source'" == "`icd_source'"	local code_version "ICD10"
			}
			** ICD9_detail
			foreach icd_source of global icd9sources {
				if "`source'" == "`icd_source'"	local code_version "ICD9_detail"
			}
			** ICD10_tabulated
			foreach icd_source of global icd10tabsources {
				if "`source'" == "`icd_source'"	local code_version "ICD10_tabulated"
			}
			** ICD10_VA
			foreach icd_source of global icd10vasources {
				if "`source'" == "`icd_source'"	local code_version "INDEPTH_ICD10_VA"
			}
			** Submit job
				!qsub -P proj_codprep -pe multi_slot 5 -l mem_free=10g -N "CoD_05_`source'" -hold_jid "CoD_04_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$prog_dir/redistribution/code/cod_rdp_master.do" "$username `source' `code_version' $timestamp"
			}
		}
	}

** *************************************************
** Step 06_hiv: Final series of adjustments to whole appended datasets
** *************************************************

	if $step_06_hiv == 1 {
		if $step_05_rdp == 0 {
			foreach source of local sources {
					!qsub -P proj_codprep -pe multi_slot 9 -l mem_free=18g -N "CoD_06_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$prog_dir/hiv_correction/reallocation_program/code/hiv_correction_master.do" "`source' $timestamp"
			}
		}
		else if $step_05_rdp == 1 {
			foreach source of local sources {
					!qsub -P proj_codprep -pe multi_slot 9 -l mem_free=18g -N "CoD_06_`source'" -hold_jid "CoD_05*`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$prog_dir/hiv_correction/reallocation_program/code/hiv_correction_master.do" "`source' $timestamp"
			}
		}
	}


** *************************************************
** Step 07_mergelong: Final series of adjustments to whole appended datasets
** *************************************************

	if $step_07_mergelong == 1 {
		if $step_06_hiv == 0 {
			foreach source of local sources {
					!qsub -P proj_codprep -pe multi_slot 8 -l mem_free=16g -N "CoD_07_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$database_dir/code/merge_long.do" "`source' $timestamp $username"
			}
		}
		else if $step_06_hiv == 1 {
			foreach source of local sources {
					!qsub -P proj_codprep -pe multi_slot 8 -l mem_free=16g -N "CoD_07_`source'" -hold_jid "CoD_06*`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$database_dir/code/merge_long.do" "`source' $timestamp $username"
			}
		}
	}


** *************************************************
** Step 08_recode: Final series of adjustments to whole appended datasets
** *************************************************

	if $step_08_recode == 1 {
		if $step_07_mergelong == 0 {
			foreach source of local sources {
					!qsub -P proj_codprep -pe multi_slot 8 -l mem_free=16g -N "CoD_08_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$database_dir/code/recode.do" "`source' $timestamp $username"
			}
		}
		else if $step_07_mergelong == 1 {
			foreach source of local sources {
					!qsub -P proj_codprep -pe multi_slot 8 -l mem_free=16g -N "CoD_08_`source'" -hold_jid "CoD_07*`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$database_dir/code/recode.do" "`source' $timestamp $username"
			}
		}
	}


** *************************************************
** Step 09_adjust: Final series of adjustments to whole appended datasets
** *************************************************

	if $step_09_adjust == 1 {
		if $step_08_recode == 0 {
			foreach source of local sources {
					!qsub -P proj_codprep -pe multi_slot 8 -l mem_free=16g -N "CoD_09_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$database_dir/code/adjust.do" "`source' $timestamp $username"
			}
		}
		else if $step_08_recode == 1 {
			foreach source of local sources {
					!qsub -P proj_codprep -pe multi_slot 8 -l mem_free=16g -N "CoD_09_`source'" -hold_jid "CoD_08*`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$database_dir/code/adjust.do" "`source' $timestamp $username"
			}
		}
	}


** *************************************************
** Step 10_aggregate: Final series of adjustments to whole appended datasets
** *************************************************

	if $step_10_aggregate == 1 {
		if $step_09_adjust == 0 {
			foreach source of local sources {
					!qsub -P proj_codprep -pe multi_slot 8 -l mem_free=16g -N "CoD_10_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$database_dir/code/aggregation.do" "`source' $timestamp $username"
			}
		}
		else if $step_09_adjust == 1 {
			foreach source of local sources {
					!qsub -P proj_codprep -pe multi_slot 8 -l mem_free=16g -N "CoD_10_`source'" -hold_jid "CoD_09*`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$database_dir/code/aggregation.do" "`source' $timestamp $username"
			}
		}
	}


** *************************************************
** Step 11_nra: Final series of adjustments to whole appended datasets
** *************************************************

	if $step_11_nra == 1 {
		if $step_10_aggregate == 0 {
			foreach source of local sources {
					!qsub -P proj_codprep -pe multi_slot 20 -l mem_free=40g -N "CoD_11_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$database_dir/code/launch_noise_reduction.do" "`source' $timestamp $username"
			}
		}
		else if $step_10_aggregate == 1 {
			foreach source of local sources {
					!qsub -P proj_codprep -pe multi_slot 20 -l mem_free=40g -N "CoD_11_`source'" -hold_jid "CoD_10*`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$database_dir/code/launch_noise_reduction.do" "`source' $timestamp $username"
			}
		}
	}


** *************************************************
** Step 12_clean: Final series of adjustments to whole appended datasets
** *************************************************

	if $step_12_clean == 1 {
		if $step_11_nra == 0 {
			foreach source of local sources {
					!qsub -P proj_codprep -pe multi_slot 8 -l mem_free=16g -N "CoD_12_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$database_dir/code/final_clean.do" "`source' $timestamp $username"
			}
		}
		else if $step_11_nra == 1 {
			foreach source of local sources {
					!qsub -P proj_codprep -pe multi_slot 8 -l mem_free=16g -N "CoD_12_`source'" -hold_jid "CoD_11*`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$database_dir/code/final_clean.do" "`source' $timestamp $username"
			}
		}
	}


** *************************************************
** Step 13_upload: Upload final series of adjustments to the CoD database
** *************************************************

	if $step_13_upload == 1 {
		if $step_12_clean == 0 {
			foreach source of local sources {
					!qsub -P proj_codprep -pe multi_slot 8 -l mem_free=16g -N "CoD_13_`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$database_dir/code/upload.do" "`source' $timestamp $username"
			}
		}
		else if $step_12_clean == 1 {
			foreach source of local sources {
					!qsub -P proj_codprep -pe multi_slot 8 -l mem_free=16g -N "CoD_13_`source'" -hold_jid "CoD_12*`source'" "$prog_dir/prep/code/shellstata13_${username}.sh" "$database_dir/code/upload.do" "`source' $timestamp $username"
			}
		}
	}

** *************************************************
** Check that runs were successful before running compile
** *************************************************
noisily display in red "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
noisily display in red "+++++++++++++++++++++| CHECK FOR STEP COMPLETION |+++++++++++++++++++++", _newline
sleep 10000
	if $step_00_format == 1 {
		noisily display in red "+++++++++++++++++++++| 00:FORMATTED |+++++++++++++++++++++", _newline
		foreach source of local sources {
			capture confirm file "$source_dir/`source'/data/intermediate/00_formatted.dta"
			if _rc == 601 noisily display "Started searching for formatted `source' at `c(current_time)'"
			while _rc == 601 {
				capture confirm file "$source_dir/`source'/data/intermediate/00_formatted.dta"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "FORMATTED: `source'"
			}
		}
	}
	
	if $step_01_map == 1 {
		noisily display in red "+++++++++++++++++++++| 01 MAPPED |+++++++++++++++++++++", _newline
		foreach source of local sources {
			capture confirm file "$source_dir/`source'/data/intermediate/01_mapped.dta"
			if _rc == 601 noisily display "Started searching for mapped `source' at `c(current_time)'"
			while _rc == 601 {
				capture confirm file "$source_dir/`source'/data/intermediate/01_mapped.dta"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "MAPPED: `source'"
			}
		}
	}
	
	if $step_02a_before_agesex == 1 {
		noisily display in red "+++++++++++++++++++++| 02a BEFORE AGE-SEX-SPLIT CORRECTIONS |+++++++++++++++++++++", _newline
		foreach source of local sources {
			if inlist("`source'", "India_CRS", "India_SCD_states_rural") {
				capture confirm file "$source_dir/`source'/data/intermediate/02a_before_agesex.dta"
				if _rc == 601 noisily display "Started searching for before_agesex `source' at `c(current_time)'"
				while _rc == 601 {
					capture confirm file "$source_dir/`source'/data/intermediate/02a_before_agesex.dta"
					sleep 1000
				}
				if _rc == 0 {
					noisily display "READY FOR AGE-SEX-SPLIT: `source'"
				}
			}
			else {
				noisily display "READY FOR AGE-SEX-SPLIT: `source'"
			}
		}
	}

	if $step_02_agesexsplit == 1 {
		noisily display in red "+++++++++++++++++++++| 02 AGE-SEX-SPLIT |+++++++++++++++++++++", _newline
		foreach source of local sources {
			capture confirm file "$source_dir/`source'/data/intermediate/02_agesexsplit.dta"
			if _rc == 601 noisily display "Started searching for age-sex-split `source' at `c(current_time)'"
			while _rc == 601 {
				capture confirm file "$source_dir/`source'/data/intermediate/02_agesexsplit.dta"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "AGE-SEX-SPLIT: `source'"
			}
		}
	}

	if $step_03_agesexrestrict == 1 {
		noisily display in red "+++++++++++++++++++++| 03 RESTRICTION CORRECTED |+++++++++++++++++++++", _newline
		foreach source of local sources {
			capture confirm file "$source_dir/`source'/data/intermediate/03_corrected_restrictions.dta"
			if _rc == 601 noisily display "Started searching for restriction corrected `source' at `c(current_time)'"
			while _rc == 601 {
				capture confirm file "$source_dir/`source'/data/intermediate/03_corrected_restrictions.dta"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "RESTRICTION CORRECTED: `source'"
			}
		}
	}

	if $step_04_before_rdp == 1 {
		noisily display in red "+++++++++++++++++++++| 04 BEFORE RDP CORRECTIONS |+++++++++++++++++++++", _newline
		foreach source of local sources {
			capture confirm file "$source_dir/`source'/data/intermediate/04_before_redistribution.dta"
			if _rc == 601 noisily display "Started searching for before RDP `source' at `c(current_time)'"
			while _rc == 601 {
				capture confirm file "$source_dir/`source'/data/intermediate/04_before_redistribution.dta"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "PREPPED FOR RDP: `source'"
			}
		}
	}

	if $step_05_rdp == 1 {
		noisily display in red "+++++++++++++++++++++| 05 REDISTRIBUTED |+++++++++++++++++++++", _newline
		foreach source of local sources {
			capture confirm file "$source_dir/`source'/data/final/05b_for_compilation.dta"
			if _rc == 601 noisily display "Started searching for redistributed `source' at `c(current_time)'"
			while _rc == 601 {
				capture confirm file "$source_dir/`source'/data/final/05b_for_compilation.dta"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "REDISTRIBUTED: `source'"
			}
		}
	}

	if $step_06_hiv == 1 {
		noisily display in red "+++++++++++++++++++++| 06 HIV CORRECTED |+++++++++++++++++++++", _newline
		foreach source of local sources {
			capture confirm file "$source_dir/`source'/data/final/06_hiv_corrected.dta"
			if _rc == 601 noisily display "Started searching for HIV corrected `source' at `c(current_time)'"
			while _rc == 601 {
				capture confirm file "$source_dir/`source'/data/final/06_hiv_corrected.dta"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "HIV CORRECTED: `source'"
			}
		}
	}

	if $step_07_mergelong == 1 {
		noisily display in red "+++++++++++++++++++++| 07 MERGED AND LONG |+++++++++++++++++++++", _newline
		foreach source of local sources {
			capture confirm file "$source_dir/`source'/data/final/07_merged_long.dta"
			if _rc == 601 noisily display "Started searching for merged `source' at `c(current_time)'"
			while _rc == 601 {
				capture confirm file "$source_dir/`source'/data/final/07_merged_long.dta"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "MERGED AND LONG: `source'"
			}
		}
	}

	if $step_08_recode == 1 {
		noisily display in red "+++++++++++++++++++++| 08 RECODED |+++++++++++++++++++++", _newline
		foreach source of local sources {
			capture confirm file "$source_dir/`source'/data/final/08_recoded.dta"
			if _rc == 601 noisily display "Started searching for recoded `source' at `c(current_time)'"
			while _rc == 601 {
				capture confirm file "$source_dir/`source'/data/final/08_recoded.dta"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "RECODED: `source'"
			}
		}
	}

	if $step_09_adjust == 1 {
		noisily display in red "+++++++++++++++++++++| 09 ADJUSTED |+++++++++++++++++++++", _newline
		foreach source of local sources {
			capture confirm file "$source_dir/`source'/data/final/05b_for_compilation.dta"
			if _rc == 601 noisily display "Started searching for ADJUSTED `source' at `c(current_time)'"
			while _rc == 601 {
				capture confirm file "$source_dir/`source'/data/final/05b_for_compilation.dta"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "ADJUSTED: `source'"
			}
		}
	}

	if $step_10_aggregate == 1 {
		noisily display in red "+++++++++++++++++++++| 10 AGGREGATED |+++++++++++++++++++++", _newline
		foreach source of local sources {
			capture confirm file "$source_dir/`source'/data/final/10_aggregated.dta"
			if _rc == 601 noisily display "Started searching for aggregated `source' at `c(current_time)'"
			while _rc == 601 {
				capture confirm file "$source_dir/`source'/data/final/10_aggregated.dta"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "AGGREGATED: `source'"
			}
		}
	}

	if $step_11_nra == 1 {
		noisily display in red "+++++++++++++++++++++| 11 NOISE REDUCED |+++++++++++++++++++++", _newline
		foreach source of local sources {
			capture confirm file "$source_dir/`source'/data/final/11_noise_reduced.dta"
			if _rc == 601 noisily display "Started searching for noise reduced `source' at `c(current_time)'"
			while _rc == 601 {
				capture confirm file "$source_dir/`source'/data/final/11_noise_reduced.dta"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "NOISE REDUCED: `source'"
			}
		}
	}

	if $step_12_clean == 1 {
		noisily display in red "+++++++++++++++++++++| 12 CLEANED |+++++++++++++++++++++", _newline
		foreach source of local sources {
			capture confirm file "$source_dir/`source'/data/final/12_cleaned.dta"
			if _rc == 601 noisily display "Started searching for cleaned `source' at `c(current_time)'"
			while _rc == 601 {
				capture confirm file "$source_dir/`source'/data/final/12_cleaned.dta"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "CLEANED: `source'"
			}
		}
	}

	if $step_13_upload == 1 {
		noisily display in red "+++++++++++++++++++++| 13 UPLOAD |+++++++++++++++++++++", _newline
		foreach source of local sources {
			capture confirm file "/ihme/cod/prep/01_database/13_upload/data/signal_`source'_complete.dta"
			if _rc == 601 noisily display "Started searching for uploaded `source' at `c(current_time)'"
			while _rc == 601 {
				capture confirm file "/ihme/cod/prep/01_database/13_upload/data/signal_`source'_complete.dta"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "UPLOADED: `source'"
			}			
		}
	}

** *************************************************
** Sweep: Clear all the tempfile in clustertmp from the prep process
** *************************************************

	if $sweep == 1 & "$source" == "" {
		!rm -r "/ihme/cod/prep/01_database/"
	}

** *************************************************
** Mark finished: check off run as successful in the excel doc
** *************************************************

	import excel using "$j/WORK/03_cod/00_documentation/COD_PREP_RECORD_GBD2015.xls", firstrow allstring clear
	replace success = "SUCCESS" if run_date == "$date" & run_time =="`time'"
	export excel using "$j/WORK/03_cod/00_documentation/COD_PREP_RECORD_GBD2015.xls", firstrow(variables) replace

}

//       @@@@@@@@@@@@@@@@@@
//     @@@@@@@@@@@@@@@@@@@@@@@
//    @@@@@@@@@@@@@@@@@@@@@@@@@@@
//   @@@@@@@@@@@@@@@@@@@@@@@@@@@@@
//  @@@@@@@@@@@@@@@/      \@@@/   @
// @@@@@@@@@@@@@@@@\      @@  @___@
// @@@@@@@@@@@@@ @@@@@@@@@@  | \@@@@@
// @@@@@@@@@@@@@ @@@@@@@@@\__@_/@@@@@
//  @@@@@@@@@@@@@@@/,/,/./'/_|.\'\,\
//    @@@@@@@@@@@@@|  | | | | | | | |
//                  \_|_|_|_|_|_|_|_|


// 				EL FIN


capture log close