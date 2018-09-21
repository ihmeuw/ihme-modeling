** ****************************************************
** Purpose: Prepare and compile all final Cause of Death sources for CoD database
** ****************************************************
// Set up
	if c(os) == "Unix" {
		set mem 10G
		set odbcmgr unixodbc
		global j "/home/j"
		local cwd = c(pwd)
		cd "~"
		global h = c(pwd)
		cd `cwd'
	}
	else if c(os) == "Windows" {
		global j "J:"
		global h "H:"
	}

** The following fields should be filled by the operator
** Operator: your User name
	global username "strUser"

** Sources: leave blank for all sources. Otherwise, space separated list		
	global source "strDataSource"
	

	global notes "strNotes"
	
** Switches
** yes (1) no (0)
	** Format datasource
	global step_00_format 			= 0
	** Merge cause map
	global step_01_map 				= 0
	** Urban-rural splitting
	global step_02a_before_agesex	= 0
	** Age-sex split the data
	global step_02_agesexsplit 		= 0
	** Correct age-sex restriction
	global step_03_agesexrestrict 	= 0
	** HIV and maternal correction
	global step_04_before_rdp 		= 0
	** Redistribution packages
	global step_05_rdp 				= 0
	** HIV Correction
	global step_06_hiv				= 0
	** Finalize: all remaining steps
	global step_07_to_13			= 0
	** Otherwise:
		** Merge 
		global step_07_mergelong		= 0
		** Age-and-sex-specific cause recode
		global step_08_recode			= 0
		** Source-type-specific adjustment
		global step_09_adjust			= 0
		** Cause aggregation
		global step_10_aggregate		= 0
		** Noise reduction algorithms
		global step_11_nra				= 0
		** Clean all variables for upload
		global step_12_clean			= 0
		** Upload
		global step_13_upload			= 0

	** Extra: clear clustertmp. Source has to be blank to properly execute.
	global sweep 				= 0

** Shared maps: exhaustive list of sources which share maps with these parent sources.
		** Read in list
		insheet using "strDirectoryName", comma names clear
		** ICD10 based maps
		levelsof source if list == "ICD10", local(icd10sources) c
		global icd10sources "`icd10sources'"
		** ICD9_detail based maps
		levelsof source if list == "ICD9_detail", local(icd9sources) c
		global icd9sources "`icd9sources'"
		** ICD10_tabulated based maps
		levelsof source if list == "ICD10_tabulated", local(icd10tabsources) c
		global icd10tabsources "`icd10tabsources'"
		** ICD10_VA based maps
		levelsof source if list == "INDEPTH_ICD10_VA", local(icd10vasources) c
		global icd10vasources "`icd10vasources'"
		
** Launch the central prep code
  do "/$j/WORK/03_cod/01_database/02_programs/prep/code/CoD_master_prep.do"

	
