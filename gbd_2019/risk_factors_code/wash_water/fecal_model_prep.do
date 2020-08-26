// Prep fecal contamination dataset for ST-GPR modeling

// clear environment
clear all
set more off
cap restore, not

// Set relevant locals
local version 2
local in_dir "FILEPATH"
local out_dir "FILEPATH"

// Import dataset from Bain et al. 2014
	import delimited "FILEPATH", clear
	drop v*
	replace noncompliance = "" if noncompliance == "-"
	destring noncompliance, replace
	keep if regexm(jmpsourcedefinition, "Piped")
	drop if noncompliance == .
	rename (noncompliance yearofpublication country) (data year_id location_name)
	replace year_id = "1999" if year_id == "NA"
	destring year_id, replace
	gen sample_size = number * studyqualityrating
	tempfile bain
	save `bain', replace

// Import dataset with other additional water quality data points
	import delimited "FILEPATH", clear // downloaded from EU water quality website
	tempfile additional
	save `additional', replace

// Prep to merge on relevant location meta data
preserve
	get_location_metadata, location_set_id(22) clear 
	keep location_id region_id region_name super_region_id ihme_loc_id location_name
	duplicates drop location_name, force
	tempfile loc
	save `loc', replace
restore

// Clean and prep for ST-GPR
	use `bain', clear
	merge m:1 location_name using `loc', keep(1 3)
	gen variance = .
	gen age_group_id = 22
	gen sex_id = 3
	gen me_name = "prop_fecal"
	gen nid = .
	gen add_nsv = 1
	keep nid location_name location_id year_id sample_size variance me_name data age_group_id sex_id ihme_loc_id
	append using `additional'
	drop if location_id == .
	export delimited "FILEPATH", replace 


local cats "riskclassification1per100ml riskclassification110per100ml riskclassification10100per100ml riskclassification100per100ml mean geometricean"
foreach cat of local cats {
	replace `cat' = "" if `cat' == "-"
	destring `cat', replace force
}

replace riskclassification10100per100ml = 0 if riskclassification110per100ml != . & riskclassification10100per100ml == .
replace riskclassification100per100ml = 0 if riskclassification110per100ml != . & riskclassification100per100ml == .
gen prop_fecal = riskclassification110per100ml + riskclassification10100per100ml + riskclassification100per100ml
keep if regexm(jmpsourcedefinition, "Piped")

replace prop_fecal = .001 if prop_fecal == 0
replace prop_fecal = .999 if prop_fecal == 1

// Subset to observations that can be crosswalked
preserve
	keep if prop_fecal != . & mean != .
	gen logit_fecal = logit(prop_fecal)
	expand 2, gen(ind)
	replace prop_fecal = mean if ind == 1

drop if prop_fecal == .

// Prep to merge on relevant location meta data
preserve
	get_location_metadata, location_set_id(22) clear 
	keep location_id region_id region_name super_region_id ihme_loc_id location_name
	duplicates drop location_name, force
	tempfile loc
	save `loc', replace
restore

// Merge on meta data
rename country location_name
merge m:1 location_name using `loc', keep(1 3) nogen

// Outliers
drop if location_name == "Mexico" & year_id == 2007
drop if location_name == "Saudi Arabia" & year_id == 2009
drop if location_name == "Jordan" & year_id == 2010

// Prep for modeling
rename (prop_fecal yearofpublication) (data year_id)
gen sample_size = number * studyqualityrating
gen variance = .
gen age_group_id = 22
gen sex_id = 3
gen me_name = "prop_fecal"
gen nid = .
keep nid location_name location_id year_id sample_size variance me_name data age_group_id sex_id ihme_loc_id
export delimited "FILEPATH", replace 


// Prep piped water as a covariate for prop_fecal model
	// improved water model
	local base_dir "FILEPATH"
	local out_dir "FILEPATH"
	import delimited "FILEPATH", clear
	replace gpr_mean = .99 if gpr_mean >= 1
	replace gpr_mean = 1 - gpr_mean // modeled unimproved water
	rename gpr_mean gpr_imp
	duplicates drop location_id year_id, force
	tempfile improved
	save `improved', replace

	//Prep piped and merge with improved
	import delimited "FILEPATH", clear
	replace gpr_mean = .99 if gpr_mean >= 1
	rename gpr_mean gpr_piped
	duplicates drop location_id year_id, force
	merge 1:1 location_id year_id using `improved', keepusing(gpr_imp) nogen
	gen cv_piped_covar = gpr_imp * gpr_piped
	keep location_id age_group_id year_id sex_id cv_piped_covar

	// Prep to merge onto prop_fecal data and save for ST-GPR
	preserve
		import delimited "FILEPATH", clear
		tempfile fecal
		save `fecal', replace
	restore
	merge 1:m location_id year_id using `fecal', nogen
	replace me_name = "prop_fecal"
	export delimited "FILEPATH", replace
