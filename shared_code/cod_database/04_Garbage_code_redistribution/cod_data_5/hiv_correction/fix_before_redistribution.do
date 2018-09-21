** ********************************************************************************************************
** Purpose: Pre-redistribution for ICD9_BTL and HIV Correction for ICD10 and ICD9_BTL
** ********************************************************************************************************

// log using "/home/j/WORK/03_cod/01_database/02_programs/prep/user_meta_log", replace
// Username
	global user "`1'"

// Source
	global source "`2'"
	di "source $source"

// Date
	global timestamp "`3'"
	di "timestamp $timestamp"
	
// Map source
	global cod_source "`4'"
	di "cod_source $cod_source"
	
// Establish directories
	// J:drive
	if c(os) == "Windows" {
		global j "J:"
	}
	if c(os) == "Unix" {
		global j "/home/j"
	}
	
	set more off
	adopath + "$j/WORK/03_cod/01_database/02_programs/hiv_correction/rdp_proportions/code"
	
// Working directories
	global source_dir "$j/WORK/03_cod/01_database/03_datasets"

// Log our efforts
	log using "$source_dir/${source}/logs/04_pre_rdp_${source}_${timestamp}.smcl", replace
	
// Identify inputs
	di "source $source"
	di "timestamp $timestamp"
	di "cod_source $cod_source"

// Identify source list
	insheet using "$j/WORK/03_cod/01_database/02_programs/hiv_correction/VR_list.csv", comma names clear
	count if vr_list == "$source"
	local HIV_VR_count = `r(N)'
	
// Load in restricted data
	use "$source_dir/${source}/data/intermediate/03_corrected_restrictions.dta", clear

	
// Redistribute HIV garbage --> J:/WORK/03_cod/01_database/02_programs/hiv_correction/rdp_proportions/code/apply_proportions.ado
	if inlist("$cod_source","ICD9_BTL","ICD9_detail","ICD10_tabulated","ICD10","Russia_FMD_1989_1998","Russia_FMD_1999_2011") {
		apply_proportions
		collapse (sum) deaths*, by(iso3 location_id subdiv national region dev_status source source_label source_type NID list frmat im_frmat sex year cause cause_name acause) fast
	} 
	
// Reallocate garbage to HIV in a few countries
	if `HIV_VR_count' > 0 {
		do  "$j/WORK/03_cod/01_database/02_programs/hiv_correction/reallocation_program/code/hiv_correction_program.do" "PRE"
	}
	

// Save
	compress
	save "$source_dir/${source}/data/intermediate/04_before_redistribution.dta", replace
	save "$source_dir/${source}/data/intermediate/_archive/04_before_redistribution_${timestamp}.dta", replace
	capture log close
