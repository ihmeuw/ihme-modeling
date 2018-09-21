// Cleaning NLD HDR data
// DATE USERNAME
	
	clear
	set more off, perm
	pause on

local check = 0
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
	
	import delimited using "FILEPATH.CSV", delimiter(";")

// Assert that data is clean
	** sex
	assert inlist(sex,1,2)
	
	** age
	assert age < 120	
	
	expand number_raw

// Add an E to front of E-codes
	tostring ecode_icd, replace
	replace ecode_icd = "E" + ecode_icd

// New process for GBD 2016

	adopath + "`code_dir'/ado"

	replace ecode_icd = subinstr(ecode_icd, ".", "", .)

	map_icd_2016 ecode_icd, icd_vers(9) code_dir("FILEPATH")
	map_icd_2016 diagnosis_icd, icd_vers(9) code_dir("FILEPATH")

	rename cause_ecode_icd final_ecode_1
	rename cause_diagnosis_icd final_ncode_1

		
// Add characteristics of dataset
	gen iso3 = "NLD"
	gen inpatient = 1
	rename diagnosis_icd icd9_ncode
	rename ecode_icd icd9_ecode
	keep iso3 year age sex final_ecode_1 final_ncode_1 inpatient icd9_ncode icd9_ecode
	order iso3 year age sex final_ecode_1 final_ncode_1 inpatient

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
	