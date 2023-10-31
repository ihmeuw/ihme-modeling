
 
FILEPATH
set more off
clear all
 
local multiplier 0 1 2 3 4
foreach m in `multiplier' {
clear all
local first = `m'*200 + 1
set obs 200
gen values = _n + 200*`m'
levelsof values, local(l)
di "FILEPATH/draws`first'.dta"
use "FILEPATH/draws_`first'.dta", clear
foreach i in `l' {
qui merge 1:1 location_id age_group_id sex_id year_id using "FILEPATH/draws_`i'.dta", force nogen
di in red "`i'"
}
save "FILEPATH/part_`m'_draws.dta", replace
}
use "FILEPATH/part_0_draws.dta", clear
local newdraw 1 2 3 4
foreach m in `newdraw' {
qui merge 1:1 location_id age_group_id sex_id year_id using "FILEPATH/part_`m'_draws.dta", force nogen
}
saveold "FILEPATH/decomp4_draws.dta", replace
 
// We now have our master draw file, loop through to save a single CSV by location //
use "/FILEPATH/decomp4_draws.dta", clear
replace age_group_id = 235 if age_group_id == 33
 
levelsof location_id, local(wheres)
local sexes 1 2
local years 1990 1995 2000 2005 2010 2015 2019 2020 2021 2022
keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2015,  2019, 2020, 2021, 2022)
 
keep location_id year_id age_group_id sex_id morb*
forval i = 1/1000 {
local j = `i' - 1
rename morbidity_`i' paf_`j'
}
gen cause_id = 302
save "/FILEPATH/morbidity.dta", replace
foreach www of local wheres {
di "`www' Morbidity"
sort year_id age_group_id sex_id
  
outsheet age_group_id location_id year_id sex_id cause_id paf_* using "/FILEPATH/paf_yld_`www'.csv" if location_id == `www', comma replace
}
 l
use "/FILEPATH/decomp4_draws.dta", clear
replace age_group_id = 235 if age_group_id == 33
 
keep location_id year_id age_group_id sex_id mort*
forval i = 1/1000 {
local j = `i' - 1
rename mortality_`i' paf_`j'
}
gen cause_id = 302
save "/FILEPATH/mortality.dta", replace
foreach www of local wheres {
di "`www' Mortality"
sort year_id age_group_id sex_id
outsheet age_group_id location_id year_id sex_id cause_id paf_* using "/FILEPATH/paf_yll_`www'.csv" if location_id == `www', comma replace 
}
 
// Finished //

