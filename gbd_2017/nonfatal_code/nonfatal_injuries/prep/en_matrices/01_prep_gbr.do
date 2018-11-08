// Cleaning dual-coded England UTLA data
// DATE USERNAME

clear
set more off

local check = 1
if `check' == 1 {
local 1 "FILEPATH"
local 2 "FILEPATH"
}	

adopath + "FILEPATH"

** import macros
global prefix `1'
local output_data_dir `2'

import delimited "FILEPATH.CSV", delim(",") clear

* convert to year
tostring fyear, force replace
gen year = regexs(1) if regexm(fyear,"([0-9]?[0-9]?)$")
replace year = "20" + year
destring year, replace force

* convert to age
drop if ageband == "NULL"
gen age = substr(ageband, 1, 2)

destring age_start age_end, replace force

* map to e-code and n-code
rename diagcode3 ncode
rename cause ecode

map_icd_2016 ecode, icd_vers(10) code_dir("FILEPATH")
rename cause_ecode e_code
map_icd_2016 ncode, icd_vers(10) code_dir("FILEPATH")
rename cause_ncode n_code

rename value cases
gen inpatient = 1
gen iso3 = "GBR"

drop ecode* ncode* fyear ageband

export delimited "`output_data_dir'/FILEPATH.csv", replace
