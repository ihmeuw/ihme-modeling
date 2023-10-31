/*
Use this code for dropping live births
*/

local user = "`c(username)'"
local code_folder FILEPATH

global in_file `1'  // FULL filepath to .dta file to be read in
global out_file `2' // FULL filepath to save output

use "$in_file"

// prepare a csv that is stored in this repo that has the live birth codes
preserve
import delimited "`code_folder'/FILEPATH", varn(1) clear
tempfile live_births_codes
save `live_births_codes', replace
restore

rename dx_1 cause_code
merge m:1 cause_code icd_vers using `live_births_codes'
rename cause_code dx_1

drop if _merge == 3

saveold "$out_file"