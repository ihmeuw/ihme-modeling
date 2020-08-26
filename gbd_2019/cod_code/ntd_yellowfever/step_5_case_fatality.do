/*
Yellow Fever step_5_case_fatality.do
*/

*** BOILERPLATE ***
set more off, perm
clear

if c(os) == "Unix" {
    local "ADDRESS"
    set odbcmgr ADDRESS
}
else if c(os) == "Windows" {
    local "ADDRESS"
}

local inputDir FILEPATH 


*** LOAD CASE FATALITY DATA **** 	 
import delimited `inputDir'/FILEPATH, clear


*** GET DATA IN THE FORMAT THAT METAN PREFERS ***	
gen mean = numerator / denominator
gen lower = .
gen upper = .

forvalues i = 1/`=_N' {
    local n = numerator in `i'
    local d = denominator in `i'
    cii `d' `n', wilson
    replace upper = `r(ub)' in `i'
    replace lower = `r(lb)' in `i'
}


*** RUN META-ANALYSIS AND CONVERT MEAN+SE TO BETA DISTRIBUTION PARAMETERS ***		
metan mean lower upper, random nograph

local mu    = `r(ES)'
local sigma = `r(seES)'

generate alphaCf = `mu' * (`mu' - `mu'^2 - `sigma'^2) / `sigma'^2 
generate betaCf  = alpha * (1 - `mu') / `mu'


*** CLEAN UP AND SAVE ***	
keep in 1
keep alphaCf betaCf

save `inputDir'/FILEPATH, replace
