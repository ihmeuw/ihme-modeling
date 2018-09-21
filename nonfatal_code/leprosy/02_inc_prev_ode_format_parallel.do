// **********************************************************************
// Purpose:        This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:         USERNAME

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "USERNAME"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "USERNAME"
	}
	
	// Define arguments
	if "`1'" != "" {
		local location_id `1'
		local min_year `2'
		local tmp_dir `3'	
	}
	else if "`1'" == "" {
		local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
		local date = subinstr("`date'"," ","_",.)
		local location_id 43911
		local min_year 1987
		local tmp_dir "FILENAME"
	}

// *********************************************************************************************************************************************************************
// Import functions
  adopath + FILENAME
	
// Get IHME loc ID for location
	get_demographics, gbd_team(epi) clear
	local year_ids `r(year_ids)'
	local sex_ids `r(sex_ids)'
	use "`tmp_dir'/loc_iso3.dta" if location_id == `location_id', clear
	levelsof iso3, local(iso) c
	local sex_1 "male"
	local sex_2 "female"
	
// Make function for fixing ages
	cap program drop age_to_age_group_id
	program define age_to_age_group_id
		version 13
		syntax , tmp_dir(string)
		recast double age
		replace age = 0.01 if age > 0.009 & age < 0.011
	    replace age = 0.1 if age > 0.09 & age < 0.11
	    merge m:1 age using "`tmp_dir'/age_map.dta", assert(3) nogen
		drop age
	end
	
// *********************************************************************************************************************************************************************

	** INCIDENCE AND PREVALENCE OF LEPROSY
	clear
	** set up empty file to append
	tempfile all_files
	save `all_files', replace emptyok

	foreach year_id of local year_ids {
		foreach sex_id of local sex_ids {
			import delimited "`tmp_dir'/draws/cases/inc_annual/incidence_`iso'_`year_id'_`sex_`sex_id''.csv", clear
			gen sex_id = `sex_id'
			gen year_id = `year_id'
			gen location_id = `location_id'
			gen measure_id = 6
			gen modelable_entity_id = 1662
			age_to_age_group_id, tmp_dir(`tmp_dir')
			keep modelable_entity_id location_id year_id age_group_id sex_id measure_id draw*
			order modelable_entity_id location_id year_id age_group_id sex_id measure_id draw*
			append using `all_files'
			save `all_files', replace
			import delimited "`tmp_dir'/draws/cases/final/prevalence_`iso'_`year_id'_`sex_`sex_id''.csv", clear
			gen sex_id = `sex_id'
			gen year_id = `year_id'
			gen location_id = `location_id'
			gen measure_id = 5
			gen modelable_entity_id = 1662
			age_to_age_group_id, tmp_dir(`tmp_dir')
			keep modelable_entity_id location_id year_id age_group_id sex_id measure_id draw*
			order modelable_entity_id location_id year_id age_group_id sex_id measure_id draw*
			append using `all_files'
			save `all_files', replace
		}
	}
	export delimited "`tmp_dir'/draws/upload/1662/`location_id'.csv", replace

	** INCIDENCE AND PREVALENCE OF DISFIGUREMENT 1
	clear
	** set up empty file to append
	tempfile all_files
	save `all_files', replace emptyok

	foreach year_id of local year_ids {
		foreach sex_id of local sex_ids {
			import delimited "`tmp_dir'/draws/disfigure_1/final/incidence_`iso'_`year_id'_`sex_`sex_id''.csv", clear
			gen sex_id = `sex_id'
			gen year_id = `year_id'
			gen location_id = `location_id'
			gen measure_id = 6
			gen modelable_entity_id = 1663
			age_to_age_group_id, tmp_dir(`tmp_dir')
			keep modelable_entity_id location_id year_id age_group_id sex_id measure_id draw*
			order modelable_entity_id location_id year_id age_group_id sex_id measure_id draw*
			append using `all_files'
			save `all_files', replace
			import delimited "`tmp_dir'/draws/disfigure_1/final/prevalence_`iso'_`year_id'_`sex_`sex_id''.csv", clear
			gen sex_id = `sex_id'
			gen year_id = `year_id'
			gen location_id = `location_id'
			gen measure_id = 5
			gen modelable_entity_id = 1663
			age_to_age_group_id, tmp_dir(`tmp_dir')
			keep modelable_entity_id location_id year_id age_group_id sex_id measure_id draw*
			order modelable_entity_id location_id year_id age_group_id sex_id measure_id draw*
			append using `all_files'
			save `all_files', replace
		}
	}
	export delimited "`tmp_dir'/draws/upload/1663/`location_id'.csv", replace	

	** INCIDENCE AND PREVALENCE OF DISFIGUREMENT 2
	clear
	** set up empty file to append
	tempfile all_files
	save `all_files', replace emptyok

	foreach year_id of local year_ids {
		foreach sex_id of local sex_ids {
			import delimited "`tmp_dir'/draws/disfigure_2/final/incidence_`iso'_`year_id'_`sex_`sex_id''.csv", clear
			gen sex_id = `sex_id'
			gen year_id = `year_id'
			gen location_id = `location_id'
			gen measure_id = 6
			gen modelable_entity_id = 1664
			age_to_age_group_id, tmp_dir(`tmp_dir')
			keep modelable_entity_id location_id year_id age_group_id sex_id measure_id draw*
			order modelable_entity_id location_id year_id age_group_id sex_id measure_id draw*
			append using `all_files'
			save `all_files', replace
			import delimited "`tmp_dir'/draws/disfigure_2/final/prevalence_`iso'_`year_id'_`sex_`sex_id''.csv", clear
			gen sex_id = `sex_id'
			gen year_id = `year_id'
			gen location_id = `location_id'
			gen measure_id = 5
			gen modelable_entity_id = 1664
			age_to_age_group_id, tmp_dir(`tmp_dir')
			keep modelable_entity_id location_id year_id age_group_id sex_id measure_id draw*
			order modelable_entity_id location_id year_id age_group_id sex_id measure_id draw*
			append using `all_files'
			save `all_files', replace
		}
	}
	export delimited "`tmp_dir'/draws/upload/1664/`location_id'.csv", replace	

// *********************************************************************************************************************************************************************