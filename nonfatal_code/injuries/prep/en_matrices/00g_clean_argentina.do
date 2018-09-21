// Cleaning Argentina hospital data
// DATE USERNAME
	
	clear
	set more off, perm
	pause on

// import macros
global prefix `1'
local code_dir `2'
local output_data_dir `3'
local log_dir `4'

if "`code_dir'" == "" {
	global prefix FILEPATH
	local code_dir "FILEPATH"
	local output_data_dir "FILEPATH"
	local log_dir ""
}
// Bring in ICD mapping ado
	adopath + "`code_dir'/ado"
	
// Append all years
local input_dir "$prefix/FILEPATH"
local years: dir "`input_dir'" dirs "20*"
tempfile alldata
foreach i of local years {
	di "`i'"
	if `i' > 2006 & `i' != 2012 {
		local fname: dir "`input_dir'/`i'" files "FILEPATH*FILEPATH*.DTA", respectcase
		local fname = subinstr(`"`fname'"',`"""',"",.)
		use "`input_dir'/`i'/`fname'", clear
		drop procquir
		cap destring muer, replace
		cap rename codcauex codcauext
		cap rename anioinfo year
		cap rename anioinfor year
		cap confirm file `alldata'
		if !_rc append using `alldata'
		save `alldata', replace
		}
}


// Rename variables of interest, drop bad sexes/ages, drop patients who died in hospital
	rename edading age
		drop if age > 120
	rename codsexo sex
		destring sex, replace
		keep if sex == 1 | sex == 2
	rename codcauext ecode_icd10
	rename coddiagpr ncode_icd10
	rename muer died_in_hospital
	drop if died_in_hospital == 1

	
// Map ICD-codes
	map_icd_2016 ncode_icd10, icd_vers(10) code_dir(`code_dir')
	map_icd_2016 ecode_icd10, icd_vers(10) code_dir(`code_dir')

	rename cause_ncode_icd10 final_ncode_1
	rename cause_ecode_icd10 final_ecode_1
		
// Add characteristics of dataset
	gen iso3 = "ARG"
	gen inpatient = 1
	keep iso3 year age sex final_ecode_1 final_ncode_1 inpatient ncode_icd10 ecode_icd10
	order iso3 year age sex final_ecode_1 final_ncode_1 inpatient ncode_icd10 ecode_icd10

// Drop garbage codes, N-codes that are coded as E-codes, E-codes that are coded as N-codes -- only keep obs with an N-code and E-code
	keep if final_ecode_1 != "" & final_ncode_1 != ""
		drop if final_ecode_1 == "_gc"
		drop if final_ncode_1 == "_gc"
	drop if regexm(final_ecode_1, "N")
	gen keep = substr(final_ncode_1, 4, 1)
	keep if keep == ""
	drop keep
	
// Save cleaned data
	export delimited using "`output_data_dir'/FILEPATH.csv", delim(",") replace
	