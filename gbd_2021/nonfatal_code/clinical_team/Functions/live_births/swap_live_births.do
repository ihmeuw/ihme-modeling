/*
Stata code for swapping live births

Can be ran on a dta file via command line arguments but this is meant to be used as a template

Assumptions about the format of the data that this code makes:
 - There are columns that start with string "dx" that are numbered and they contain ICD codes. E.g. dx1, dx2, etc. This is how raw HCUP data looks.
 - There is a column icd_vers with values 'ICD9_detail' for ICD 9 and values 'ICD10' for ICD 10
*/

// this is very important! otherwise stata will interpret column dx_1 as being short for dx_10, dx_11, etc.
set varabbrev off
set more off

local user = "`c(username)'"
local code_folder FILEPATH

global in_file `1'  // FULL filepath to .dta file to be read in
global out_file `2' // FULL filepath to save output

use "$in_file"

// Assume that diagnosis columns have names that start with dx and then have a number
// E.g. dx1, dx2, etc. This is how raw HCUP data looks
// This inserts an underscore, so we end up with dx_1, dx_2, etc.
forvalues n = 1(1)100 {
    capture rename dx`n' dx_`n'
}

// copy dx_1
clonevar new_dx_1 = dx_1

// prepare a csv that is stored in this repo that has the live birth codes
preserve
import delimited "`code_folder'/FILEPATH", varn(1) clear
tempfile live_births_codes
save `live_births_codes', replace
restore

// NOTE
// the merge creates a column name '_merge' by default
// its values are:
// 1: from master, no matches
// 2: from using, no matches
// 3: in both sides of the merge, i.e., matched
// For example, you can use the column _merge like this:
// count if _merge == 3
rename dx_1 cause_code
merge m:1 cause_code icd_vers using `live_births_codes'
rename cause_code dx_1

// make a mask that shows where dx_1 has a live birth code
gen primary_dx_is_live_birth = 1 if _merge == 3
replace primary_dx_is_live_birth = 0 if primary_dx_is_live_birth == .
drop _merge

// make list of numbers representing each of the dx columns
// NOTE, only the diagnosis columns can be named dx_ for this to work
local dx_count = 0
foreach var of varlist dx_* {
    local dx_count = `dx_count' + 1
}
display `dx_count'


gen is_live_birth = .
forvalues dx = `dx_count'(-1)1 {
    replace is_live_birth = .
    display `dx'
    rename dx_`dx' cause_code
    merge m:1 cause_code icd_vers using `live_births_codes'
    rename cause_code dx_`dx'
    replace is_live_birth = 1 if _merge == 3
    replace is_live_birth = 0 if is_live_birth == .
    noisily replace new_dx_1 = dx_`dx' if dx_`dx' != "" & is_live_birth == 0 & primary_dx_is_live_birth == 1
    drop _merge
}

// make final replacement
count if dx_1 != new_dx_1
drop dx_1
rename new_dx_1 dx_1


saveold "$out_file"