//Prep Stata
	clear all
	set more off
	set maxvar 32767 
	
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}
// Add adopaths
	adopath + "FILEPATH"
	
// Pull in age group information
	get_ids, table(age_group)
	split age_group_name, parse(" to ")
	rename age_group_name1 age_start
	rename age_group_name2 age_end
	
	replace age_start="0" if age_group_id==2
	replace age_start="0.01917808" if age_group_id==3
	replace age_start="0.07671233" if age_group_id==4
	
	replace age_end="0.01917808" if age_group_id==2
	replace age_end="0.07671233" if age_group_id==3
	replace age_end="1" if age_group_id==4
	
	replace age_start="95" if age_group_id==235
	replace age_end="100" if age_group_id==235
	
	keep if inlist(age_group_id, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235)  
	destring age_start, replace
	destring age_end, replace
	
	tempfile ages
	save `ages', replace
	
//Ischemic
//Pull in Rankin information
	use "FILEPATH/rankin_chronic.dta", clear
	reshape long mean lower upper, i(age_group_id sex_id location_id) j(severity) string
	merge m:1 age_group_id using `ages', keep(3) nogen

//Recodes
	generate target_meid = 3095 if severity=="_asymp"
	replace target_meid = 1833 if severity=="_mild"
	replace target_meid = 1834 if severity=="_mod"
	replace target_meid = 1835 if severity=="_mod_cog"
	replace target_meid = 1836 if severity=="_sev"
	replace target_meid = 1837 if severity=="_sev_cog"
	
	generate year_start = 1990
	generate year_end = 2016
	generate source_meid = 10837
	
//Drop unneeded variables and reorder for export
	keep source_meid target_meid location_id year_start year_end age_start age_end sex_id mean lower upper
	
	order source_meid target_meid location_id year_start year_end age_start age_end sex_id mean lower upper
	
//Save file
	export excel using "FILEPATH/chronic_ischemic_severity_splits.xlsx", firstrow(variables) replace 
	
//Hemorrhagic
//Pull in Rankin information
	use "FILEPATH/rankin_chronic.dta", clear
	reshape long mean lower upper, i(age_group_id sex_id location_id) j(severity) string
	merge m:1 age_group_id using `ages', keep(3) nogen

//Recodes
	generate target_meid = 3096 if severity=="_asymp"
	replace target_meid = 1845 if severity=="_mild"
	replace target_meid = 1846 if severity=="_mod"
	replace target_meid = 1847 if severity=="_mod_cog"
	replace target_meid = 1848 if severity=="_sev"
	replace target_meid = 1849 if severity=="_sev_cog"
	
	generate year_start = 1990
	generate year_end = 2016
	generate source_meid = 9311
	
//Drop unneeded variables and reorder for export
	keep source_meid target_meid location_id year_start year_end age_start age_end sex_id mean lower upper
	
	order source_meid target_meid location_id year_start year_end age_start age_end sex_id mean lower upper
	
//Save file
	export excel using "FILEPATH/chronic_hemorrhagic_severity_splits.xlsx", firstrow(variables) replace 
	
