// Cleaning CHN data
// DATE USERNAME
	
	clear
	set more off, perm

local check = 99
if `check' == 1 {
local 1 "FILEPATH"
local 2 "FILEPATH"
local 3 "FILEPATH"
local 4 "FILEPATH"
}	
	
// import macros
global prefix `1'
local code_dir `2'
local output_data_dir `3'
local log_dir `4'

// Settings
	local input_dir "FILEPATH"
	

// Append datasets
	local files: dir "`input_dir'" files "*.DTA", respectcase
	local files = subinstr(`"`files'"',`"""',"",.)
	clear
	cap erase `appended'
	tempfile appended
	local ds 0
	foreach f of local files {
	* Import
		use  "`input_dir'/`f'", clear
		cap gen year = year(admissiondate)
		tostring id, replace
		replace id = "ds_`ds'_" + id
		keep if icd10_stcode != "" & icd10_vycode != ""
		cap keep id sex year age icd10*
		if _rc keep id sex age icd10*
		
	* Append
		cap confirm file `appended'
		if !_rc append using `appended'
		save `appended', replace
		
	* go to next dataset
		local ++ds
	}

// Assert that data is clean
	** sex
	assert inlist(sex,1,2)
	
	** age
	assert age < 120

// Bring in ICD mapping ado
	adopath + "FILEPATH"
	
// Map to NEW GBD 2016 ICD CODES!

	replace icd10_stcode = subinstr(icd10_stcode, ".", "", .)
	replace icd10_vycode = subinstr(icd10_vycode, ".", "", .)

	map_icd_2016 icd10_stcode, icd_vers(10) code_dir("FILEPATH")
	map_icd_2016 icd10_vycode, icd_vers(10) code_dir("FILEPATH")

	rename cause_icd10_stcode final_ncode_1
	rename cause_icd10_vycode final_ecode_1

// Add characteristics of dataset
	gen iso3 = "CHN"
	gen inpatient = 0
	rename icd10_stcode icd10_ncode
	rename icd10_vycode icd10_ecode
	keep iso3 year age sex final_ecode_1 final_ncode_1 inpatient icd10_ncode icd10_ecode
	order iso3 year age sex final_ecode_1 final_ncode_1 inpatient icd10_ncode icd10_ecode

// Drop garbage codes, N-codes that are coded as E-codes, E-codes that are coded as N-codes -- only keep obs with an N-code and E-code
	keep if final_ecode_1 != "" & final_ncode_1 != ""
		drop if final_ecode_1 == "_gc"
		drop if final_ncode_1 == "_gc"
	drop if regexm( final_ecode_1, "N")
	gen keep = substr( final_ncode_1, 4, 1)
	keep if keep == ""
	drop keep
	
// Save cleaned data
	export delimited using "`output_data_dir'/FILEPATH.csv", delim(",") replace
	