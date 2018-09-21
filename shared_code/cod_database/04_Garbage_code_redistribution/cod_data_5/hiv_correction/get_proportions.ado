** Purpose: Generate proportions to redistribute HIV deaths

** ***************************
** Define program
** ***************************
cap program drop get_proportions
program define get_proportions, nclass
version 13

** ***************************
** Set up
** ***************************
// Files and directories
	if c(os) == "Windows" {
		di in red "No longer works on Windows"
		BREAK
	}

	local rdp_dir "/ihme/cod/prep/01_database/02_programs/hiv_correction/rdp_proportions/data"
	local pkg_dir "~/cod-data/01_inputs/hiv_correction/rdp_proportions"
	capture erase "`rdp_dir'/hiv_rdp_props.dta"
	
** ***************************
** Specifications (best to use rate/age_group OR rrate/age)
** ***************************
// What rate to use (crude rates [rate] or relative rates [rrate])?
	local r_val "rate"
	
// What age variable to use (specific [age] or bands [age_group])?
	local age_val "age_group"
	
** ***************************
** Load inputs
** ***************************
preserve
// Load packages
	** ICD9 detail
	import excel using "`pkg_dir'/ICD9_detail_pkgs.xlsx", firstrow clear
	keep cause pkg remainder_cause
	gen list = "ICD9_detail"
	tempfile ICD9_detail
	save `ICD9_detail', replace
	** Russia ICD9
	replace list = "Russia_FMD_1989_1998"
	replace cause = "acause__gc_" + cause
	tempfile RUS_ICD9_tab
	save `RUS_ICD9_tab', replace
	** BTL (requires both BTL codes and ICD9 detail disaggregation targets)
	import excel using "`pkg_dir'/ICD9_BTL_pkgs.xlsx", firstrow clear
	keep cause pkg remainder_cause
	append using `RUS_ICD9_tab'
	replace list = "ICD9_BTL"
	tempfile ICD9_BTL
	save `ICD9_BTL', replace
	** ICD10 detail
	import excel using "`pkg_dir'/ICD10_pkgs.xlsx", firstrow clear
	keep cause pkg remainder_cause
	gen list = "ICD10"
	tempfile ICD10
	save `ICD10', replace
	** ICD10 tabulated
	replace cause = "acause__gc_" + cause
	replace list = "ICD10_tabulated"
	tempfile ICD10_tab
	save `ICD10_tab', replace
	** Russia ICD10
	replace list = "Russia_FMD_1999_2011"
	tempfile RUS_ICD10_tab
	save `RUS_ICD10_tab', replace
	** Compile
	use `ICD9_detail', clear
	append using `ICD9_BTL'
	append using `RUS_ICD9_tab'
	append using `ICD10'
	append using `ICD10_tab'
	append using `RUS_ICD10_tab'
	replace cause = subinstr(cause,".","",.)
	tempfile remainders
	save `remainders', replace
	keep cause list pkg
	drop if inlist(list,"ICD9_BTL","ICD10_tabulated")
	tempfile pkgs
	save `pkgs', replace

// Get envelope
	do "~/cod-data/02_programs/prep/code/env_wide.do"
	keep year iso3 location_id sex pop*
	egen pop1 = rowtotal(pop*)
	tempfile pops
	save `pops', replace
restore

** ***************************
** Run Program
** ***************************
// Use data and collapse to package level
	merge m:1 cause list using `pkgs', assert(1 3) keep(3) nogen
	fastcollapse deaths*, by(iso3 location_id region sex year pkg) type(sum)
	
// Load in population and aggregate years
	gen year_range = ""
	foreach year of numlist 1980(5)2015 {
		local year_end = `year'+4
		replace year_range = "`year'_`year_end'" if year >= `year' & year <= `year_end'
	}
	replace year_range = "1980_1984" if year < 1980
	merge m:1 year iso3 location_id sex using `pops', assert(2 3) keep(3) nogen
	
// Keep only countries present in the reference year (1980-1984) for each package
	replace iso3 = iso3 + "_" + string(location_id) if location_id != .
	levelsof iso3, local(iso3s)
	gen keep_iso = 0
	foreach iso3 of local iso3s {
		count if iso3 == "`iso3'" & year_range == "1980_1984"
		if `r(N)' > 0 {
			replace keep_iso = 1 if iso3 == "`iso3'"
		}
	}
	replace iso3 = substr(iso3,1,3)
	keep if keep_iso == 1
	
// Aggregate to region (+ global) and make long, aggregate to age bands
	fastcollapse deaths* pop*, by(region sex year_range pkg) type(sum)
	preserve
		replace region = 1
		fastcollapse deaths* pop*, by(region sex year_range pkg) type(sum)
		tempfile globalnum
		save `globalnum', replace
	restore
	append using `globalnum'
	reshape long deaths pop, i(region sex year_range pkg) j(age_code)
	keep if age_code >=3 & !inlist(age_code,4,5,6,23,24,25,26,92)
	gen double age = (age_code-6)*5
	replace age = 0 if age_code == 91
	replace age = 0.01 if age_code == 93
	replace age = 0.1 if age_code == 94
	replace age = 1 if age_code == 3
	gen age_group = "nn" if age < 0.1
	replace age_group = "1_59mo" if age >= 0.1 & age < 5
	replace age_group = "5_19" if age >= 5 & age < 20
	replace age_group = "20_49" if age >= 20 & age < 50
	replace age_group = "50_59" if age >= 50 & age < 60
	replace age_group = "60_69" if age >= 60 & age < 70
	replace age_group = "70_79" if age >= 70 & age < 80
	replace age_group = "80plus" if age >= 80
	preserve
		keep age_code age_group
		duplicates drop
		tempfile age_map
		save `age_map', replace
	restore
	fastcollapse deaths pop, by(region sex year_range pkg `age_val') type(sum)
	gen rate = deaths/pop
	drop deaths pop
	
// Make relative rate (reference is ages 70-79)
	preserve
		if "`age_val'" == "age" keep if age >= 70 & age < 80
		if "`age_val'" == "age_group" keep if age_group == "70_79"
		collapse (mean) rate, by(region sex year_range pkg) fast
		rename rate age_ref
		tempfile ageref
		save `ageref', replace
	restore
	merge m:1 region sex year_range pkg using `ageref', assert(3) nogen
	replace age_ref = .0000001 if age_ref == 0 | age_ref == .
	gen rrate = rate/age_ref
	drop age_ref
	
// Make year reference
	preserve
		keep if year_range == "1980_1984"
		drop year
		rename `r_val' year_ref
		tempfile yrref
		save `yrref', replace
	restore
	merge m:1 region sex `age_val' pkg using `yrref', keep(1 3) nogen
	replace `r_val' = .0000001 if `r_val' == 0 | `r_val' == .
	replace year_ref = .0000001 if year_ref == 0 | year_ref == .
	
// Get proportions
	gen prophiv = (`r_val'-(year_ref*1.05))/`r_val'
	replace prophiv = 0 if prophiv < 0
	gen propremainder = 1-prophiv
	drop if year_range == "1980_1984"
	
	reshape long prop, i(region sex year_range pkg `age_val') j(target) string
	keep prop region sex year_range pkg `age_val' target
	if "`age_val'" == "age" replace target = "ZZZ" if target == "hiv" & (age < 0.1 | age >= 70)
	if "`age_val'" == "age_group" replace target = "ZZZ" if target == "hiv" & inlist(age_group,"nn","70_79","80plus")
	
// Assign remainders
	joinby pkg using `remainders'
	replace target = remainder_cause if target == "remainder"
	gen acause = "_gc"
	
// Fix sex-restriction problems
	replace target = "ZZZ" if target == "gyne_candidiasis" & sex == 1
	fastcollapse prop, by(region sex year_range age_group list cause acause target) type(sum)
	
// Refit all ages
	joinby age_group using `age_map'
	drop age_group
	reshape wide prop, i(region sex year_range list cause acause target) j(age_code)
	foreach var of varlist prop* {
		replace `var' = 0 if `var' == .
	}
	
// Format and save
	sort region sex year_range list cause target
	order region sex year_range list cause acause target prop*
	rename region p_reg
	saveold "`rdp_dir'/data/hiv_rdp_props.dta", replace
	saveold "`rdp_dir'/data/_archive/hiv_rdp_props_${timestamp}.dta", replace
end
