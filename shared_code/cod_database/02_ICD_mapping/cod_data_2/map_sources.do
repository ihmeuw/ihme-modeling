** ********************************************************************************************************
** Purpose: Map formatted datasets to geographic and cause-list identifiers
** Features: 1) recognizes source_detail as an identifier for lit-review maps
**			2) Trims causes for ICD sources as needed
**			3) Save a file with missing codes that don't match the map
** ********************************************************************************************************

set more off

// Source
	global source "`1'"

// Date
	global timestamp "`2'"
	
// Establish directories
	// J:drive
	if c(os) == "Windows" {
		global j "J:"
	}
	if c(os) == "Unix" {
		global j "/home/j"
		set odbcmgr unixodbc
	}

// Working directories
	global source_dir "$j/WORK/03_cod/01_database/03_datasets"

// Log our efforts
	log using "$source_dir/${source}/logs/01_map_${source}_${timestamp}", replace

// Log our efforts
	log using "`log_dir'/01_map_${source}_${timestamp}", replace
	
// Fetch country information
	odbc load, exec("SELECT location_type, ihme_loc_id, developed, region_id FROM shared.location_hierarchy_history WHERE location_set_version_id = 34 and location_type in('admin0','admin1','nonsovereign','urbanicity', 'other') ORDER BY sort_order") (strConnection)clear

	** Reformat location identifiers to match the database
	gen iso3 = substr(ihme_loc_id, 1, 3)
	drop if developed==""
	tostring developed, replace
	replace developed = "D" + developed if substr(developed, 1, 1)!="G"
	rename developed dev_status
	rename region_id region
	drop location_type ihme_loc_id
	duplicates drop
	isid iso3
	tempfile geo
	save `geo', replace

// Load in formatted data
	use "$source_dir/${source}/data/intermediate/00_formatted.dta", clear
	
// Drop pre-1980 data, except for ICD7A and ICD8A data sources which we use for age-sex weights
	drop if year<1970 & "$source"!="ICD7A" & "$source"!="ICD8A"

// Map geographical location
	merge m:1 iso3 using `geo', keep(1 3)
	count if _m==1
	if `r(N)'>0 {
		noisily display in red "These ISO3s are dropped because they are not used in the GBD estimation process"
		noisily tab iso3 if _m==1 /* & !inlist(iso3, "ASM", "GUM", "VIR") */
	}
	drop if _m==1 & !inlist(iso3, "ASM", "GUM", "VIR", "MNP")
	drop _m

// Map to the cause list
	rename cause cause_code
	drop cause_name
	capture tostring cause_code, replace
	** drop the decimal point in the ICD data
	if list=="ICD10" | list=="ICD9_detail" | list == "INDEPTH_ICD10_VA" | source=="ICD9_BTL" | source=="ICD10" | source == "ICD8_detail" | source == "INDEPTH_ICD10_VA" {
		replace cause_code = subinstr(cause_code, ".", "", .)
		replace cause_code = subinstr(cause_code, ",", "", .)
		replace cause_code = subinstr(cause_code, " ", "", .)
	}
	
	** If the dataset has source_detail (i.e. it's a lit review), then use source_label to uniquely identify the sub-maps
	capture merge m:1 cause_code source_label using "$source_dir/${source}/maps/cause_list/map_${source}.dta", keep(1 3)

	if _rc!=0 {
		merge m:1 cause_code using "$source_dir/${source}/maps/cause_list/map_${source}.dta",  keepusing(yll_cause cause_name) keep(1 3)
		** }
		** Trim unmatched 5-digit to 4-digit if _m==1 in ICD10 and ICD9
		if list=="ICD10" | list=="ICD9_detail" | list == "INDEPTH_ICD10_VA" | source=="ICD9_BTL" | source=="ICD10" | source == "ICD8_detail" | source == "INDEPTH_ICD10_VA" {
			replace cause_code = substr(cause_code, 1, 4) if _m==1
			drop _m
			merge m:1 cause_code using "$source_dir/${source}/maps/cause_list/map_${source}.dta", update keepusing(yll_cause cause_name) keep(1 3 4 5) 
		}
		
		** Trim unmatched 4-digit to 3-digit if _m==1 in ICD10 and ICD9
		if list=="ICD10" | list=="ICD9_detail" | list == "INDEPTH_ICD10_VA" | source=="ICD9_BTL" | source=="ICD10" | source == "ICD8_detail" | source == "INDEPTH_ICD10_VA" {
			replace cause_code = substr(cause_code, 1, 3) if _m==1
			drop _m
			merge m:1 cause_code using "$source_dir/${source}/maps/cause_list/map_${source}.dta", update keepusing(yll_cause cause_name) keep(1 3 4 5)
		}
	}

	** Clean up cc_code
	replace yll_cause = "cc_code" if inlist(yll_cause, "CC Code", "CC_Code", "CC_CODE", "cc code", "CC_code", "CC code")

	** Drop Stillbirths & sub-totals
	drop if yll_cause == "_sb"
	drop if yll_cause == "sub_total"
	
	** CREATE A REPORT OF UNMATCHED CAUSES
	count if _m==1
	if `r(N)' > 0 {
		keep if _m==1
		keep cause_code cause_name source_label
		duplicates drop
		compress
		export excel using "$j/WORK/00_dimensions/03_causes/strUser_to/MISSING_CODES/_MISSING_${source}_${timestamp}.xlsx", firstrow(variables) replace
		WeRequirePerfection
		BREAK
	} 

// Save
	label drop _all
	rename cause_code cause
	rename yll_cause acause
	collapse(sum) deaths*, by(iso3 subdiv location_id national source source_label source_type NID list frmat im_frmat sex year cause cause_name acause dev_status region) fast
	compress
	save "$source_dir/${source}/data/intermediate/01_mapped.dta", replace
	save "$source_dir/${source}/data/intermediate/_archive/01_mapped_${timestamp}.dta", replace
	capture log close

	
	
