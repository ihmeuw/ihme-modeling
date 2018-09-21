// THIS PROGRAM MAPS ICD CODES TO ECODES AND NCODES
// USED IN THE INJURIES CUSTOM HOSPITAL ANALYSES

capture program drop map_icd_2016
program define map_icd_2016
	version 13
	syntax varname, Icd_version(integer) code_dir(string)

	di "Varname: `varlist'"
	di "ICD version: `icd_version'"
	di "Code Dir: `code_dir'"

// Set macros
	if `icd_version' == 9 {
		local sheet_name "ICD9"
	}
	else if `icd_version' == 10 {
		local sheet_name "ICD10"
	}

	local icd_colname "icd_code"
	local map_path "`code_dir'/FILEPATH.xlsx"

// Correct common misspecifications of ICD code
	gen `varlist'_orig = `varlist'
	cap tostring `varlist', replace
	
	** one potential source of errors in mapping is ICD10 codes with a lower case letter. This is accounted for here.
	replace `varlist' = upper(`varlist')
	replace `varlist' = subinstr(`varlist', ".", "", .)
	
	** another is ICD9 codes <100 which don't contain leading 0's
	if `icd_version' == 9 {
		replace `varlist' = "0" + `varlist' if length(`varlist')==2 & strpos(`varlist',".")==0
		replace `varlist' = "00" + `varlist' if length(`varlist')==1 & strpos(`varlist',".")==0
		replace `varlist' = "00" + `varlist' if length(`varlist')==1
		replace `varlist' = "0" + `varlist' if strpos(`varlist',".")==2
	}
	
// Save file in current format
	tempfile premap_`varlist'
	save `premap_`varlist''
	
// Get master_cause_map
	import excel using "`map_path'", sheet("`sheet_name'") firstrow case(preserve) clear
	keep `icd_colname' nonfatal_cause_name
	rename `icd_colname' `varlist'

	replace `varlist' = subinstr(`varlist', ".", "", .)
	drop if regexm(nonfatal_cause_name, "N51")
	drop if missing(`varlist')
	duplicates drop
	tempfile map
	save `map'
	
// Merge
	
	merge 1:m `varlist' using `premap_`varlist'', keep(2 3) nogen
	rename nonfatal_cause_name cause_`varlist'

	drop `varlist'

	qui {
		replace cause_`varlist' = subinstr(cause_`varlist', "E-CODE, (", "", .)
		replace cause_`varlist' = subinstr(cause_`varlist', ")", "", .)

		replace cause_`varlist' = "inj_suicide_other" if inlist(cause_`varlist', "inj_suicide_fire", "inj_suicide_pesti")
		replace cause_`varlist' = "inj_poisoning" if inlist(cause_`varlist', "inj_poisoning_gas", "inj_poisoning_pesti")
		replace cause_`varlist' = "inj_homicide_other" if inlist(cause_`varlist', "inj_homicide_sexual")
		replace cause_`varlist' = "inj_othunintent" if inlist(cause_`varlist', "inj_Electrocution")

		replace cause_`varlist' = subinstr(cause_`varlist', "N-code", "", .)
		replace cause_`varlist' = subinstr(cause_`varlist', ", (", "", .)
		replace cause_`varlist' = "N28" if cause_`varlist' == "N29"

		replace cause_`varlist' = substr(cause_`varlist', strpos(cause_`varlist', "N"), .) if !regexm(cause_`varlist', "inj_")
	}
	
	erase `premap_`varlist''

end
