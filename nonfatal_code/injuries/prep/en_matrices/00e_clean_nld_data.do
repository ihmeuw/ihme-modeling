// Cleaning NLD data
// DATE USERNAME

	clear
	set more off, perm

local check = 99
if `check' == 1 {
local 1 "FILEPATH"
local 2 "FILEPATH"
}	
	
	** import macros
global prefix `1'
local output_data_dir `2'

// Insheet codebook
	use "FILEPATH.DTA", clear
	rename ecode ecode_num
	rename acause ecode
	drop sequela
	drop if ecode_num == . | ecode_num == 99
	tempfile ecodes
	save `ecodes'
	
// Insheet raw data
	import delimited using "FILEPATH.CSV", delim(";") clear
	rename ecode ecode_num
	merge m:1 ecode_num using `ecodes'
		drop if _merge != 3
		drop _merge
	// Reformat n-codes
	tostring ncode, replace
	gen final_ncode_1 = "N" + ncode
	rename ecode final_ecode_1
	gen iso3 = "NLD"
	rename hospitalized inpatient
	keep iso3 year age sex final_ecode_1 final_ncode_1 inpatient
	order iso3 year age sex final_ecode_1 final_ncode_1 inpatient

	drop if inpatient == 1

// Save cleaned data
	export delimited using "`output_data_dir'/FILEPATH.csv", delim(",") replace
	