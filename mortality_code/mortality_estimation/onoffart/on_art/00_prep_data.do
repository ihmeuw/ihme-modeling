// Combine GBD2013 (OLD) data with GBD2015/2016 data


// Set Settings
	clear all
	set more off
	
	if (c(os)=="Unix") {
		global root FILEPATH
	}

	if (c(os)=="Windows") {
		global root FILEPATH
	}
	
	local gbd2013_dir FILEPATH
	*THIS IS ALSO 2016 DIRECTORY
	local gbd2015_dir FILEPATH
	local out_dir FILEPATH

// Set the locals for the variables you need in the data sheet
// While in theory if all extractors use the same sheet in the same order it'll be fine, we want to enforce this
	local hr_vars_1 = "author_year pubmed_id nid iso3 year_start year_end subcohort_id baseline include_any include_sex include_age why_exclude" 
	local hr_vars_2 = "prop_male age_med sample_size sex age_start age_end HR_adj std_hr_ref HR_ref HR_mort HR_mort_lo HR_mort_hi std_hr_mort std_hr_lo std_hr_hi" 
	local hr_vars_3 = "cd4_med cd4_start cd4_end treat_mo_start treat_mo_end notes page_num table_num gbd_region site cohort extractor gbd"
	local hr_vars = "`hr_vars_1' `hr_vars_2' `hr_vars_3'"

	local km_vars_1 = "why_exclude include baseline pubmed_id nid author_year subcohort_id page_num table_num gbd_region iso3 site cohort year_start year_end"
	local km_vars_2 = "sex prop_male age_med age_start age_end cd4_med cd4_start cd4_end treat_mo_start treat_mo_end sample_size"
	local km_vars_3 = "num_deaths num_trans num_LTFU LTFU_prop LTFU_prop_lo LTFU_prop_hi alive_prop alive_prop_lo alive_prop_hi dead_prop dead_prop_lo dead_prop_hi trans_prop trans_lo trans_hi"
	local km_vars_4 = "from_graph LTFU_def extractor notes ED_notes gbd LTFU_crude LTFU_invs dead_prop_alt"
	local km_vars = "`km_vars_1' `km_vars_2' `km_vars_3' `km_vars_4'"


// Bring in and format GBD 2013 data
	cd "`gbd2013_dir'"
	
	// Hazard Ratio data
	import excel using "HIV_extract_HR_compile.xlsx", firstrow clear 
	drop sub_cohort pdf_location pdf_name A* B* C*
	rename (num_study HRref subid whyexclude) (sample_size HR_ref subcohort_id why_exclude)
	gen gbd = 2013
	
	tostring subcohort_id, replace
	
	tempfile hr_2013
	save `hr_2013'

	// "Kaplan Meier" data
	import excel using "HIV_extract_KM_compile.xlsx", firstrow clear
	
	drop if classification == "rate" // Didn't end up using rates at all

	drop KM_and_LTFU_type pdf_location pdf_name person_years classification units adj years_at_risk LTFU_rt dead_rt dead_rt_lo dead_rt_hi BK BL
	rename (subid num_study whyexclude EDnotes) (subcohort_id sample_size why_exclude ED_notes)
	
	tostring subcohort_id, replace

	gen gbd = 2013
	tempfile km_2013
	save `km_2013'


// Bring in GBD 2015/2016 HR data
	cd "`gbd2015_dir'"
	import excel using "lit_extraction_hiv_onART_brownav_Y2015_2016M06D08.xlsx", firstrow clear sheet("Hazard Ratio")
	tostring subcohort_id, replace 
	gen gbd = 2015 
	replace gbd = 2016 if extractor == NAME 
	rename HRref HR_ref
	tempfile hr_2015
	save `hr_2015'
	import excel using "ARTCC_results_D08M12Y2015.xlsx", firstrow clear sheet("Hazard Ratio") cellrange(A1:AQ72)
	tostring subcohort_id, replace
	tempfile art_hr
	save `art_hr'
	use `hr_2015', clear
	append using `art_hr', force
	save `hr_2015', replace
	
// Bring in GBD2015/2016 KM data 
	import excel using "lit_extraction_hiv_onART_brownav_Y2015_2016M06D08.xlsx", firstrow clear sheet("Kaplan-Meier") 
	tostring subcohort_id, replace 
	gen gbd = 2015 
	replace gbd = 2016 if extractor == NAME
	tempfile km_2015
	save `km_2015'
	import excel using "ARTCC_results_D08M12Y2015.xlsx", firstrow clear sheet("Kaplan-Meier")
	gen gbd = 2015
	tostring subcohort_id LTFU_prop dead_prop, replace
	tempfile art_km
	save `art_km'
	use `km_2015', clear
	append using `art_km', force
	save `km_2015', replace



// Format and output data into formatted datasheet
	
	cd "`out_dir'"
	
	// Output Hazard Ratios	
	use `hr_2015', clear
	tostring year_start year_end std_hr_ref treat_mo_end, replace
	foreach var in age_med cd4_med {
		replace `var' = "" if `var' == "NULL"
		destring `var', replace
	}
	
	replace sex = "3" if sex == "both"
	replace sex = "2" if sex == "female" 
	replace sex = "1" if regexm(sex,"male")
	destring sex , replace 
	
	
	append using `hr_2013'
	
	replace baseline = 0 if baseline == .
	rename NID nid
	
	destring subcohort_id, replace force
	
	// Remove end-of-line characters from site, kills the csv.
	preserve
	describe, replace clear
	levelsof name if regexm(type,"str"), local(vars)
	restore
	foreach vvv in `vars' {
			replace `vvv' = subinstr(`vvv',char(10),"",.) 
	}

	keep `hr_vars'
	order `hr_vars'
	gen n = _n 
	sort gbd n
	drop n  
	
	export excel "HIV_extract_HR_2015.xlsx", firstrow(variables) sheet("Sheet1") replace

	
	// Output Kaplan Meier 
	use `km_2015', clear
	tostring treat_mo_end, replace
	foreach var in cd4_med LTFU_prop LTFU_prop_lo LTFU_prop_hi dead_prop dead_prop_lo dead_prop_hi {
		replace `var' = "" if `var' == "NULL" | `var' == " "
		replace `var' = subinstr(`var',"+","",.)
		destring `var', replace
	}
	replace sex = "3" if sex == "both"
	replace sex = "2" if sex == "female" 
	replace sex = "1" if sex == "male"
	destring sex, replace 
	
	append using `km_2013'
	
	rename NID nid
	replace pubmed_id = nid if pubmed_id == . & nid != . 
	destring subcohort_id, replace force

	// Remove end-of-line characters from site, kills the csv.
	preserve
	describe, replace clear
	levelsof name if regexm(type,"str"), local(vars)
	restore
	foreach vvv in `vars' {
			replace `vvv' = subinstr(`vvv',char(10),"",.) 
	}
	
	keep `km_vars'
	order `km_vars'
	gen n = _n
	sort gbd n
	drop n 
	export excel "HIV_extract_KM_2015.xlsx", firstrow(variables) sheet("HIV_extract_KM_compile") sheetreplace
