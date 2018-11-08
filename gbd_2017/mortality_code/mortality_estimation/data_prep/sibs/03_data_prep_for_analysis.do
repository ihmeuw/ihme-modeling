
**
** ******************************************************************************************************

** *****************************************************************************************
** SET UP STATA                         
** *****************************************************************************************

clear
capture clear matrix
set more off

use "FILEPATH", clear
split svy, parse("_")
rename svy1 iso3
rename svy3 surveyyear
drop svy2
destring surveyyear, replace
replace surveyyear = surveyyear - 1 
sort iso3

** *****************************************************************************************
** 1. Merge sibling information with country identifier variables 
** *****************************************************************************************
gen country = substr(svy,1,3)

** *****************************************************************************************
** 2. Append missing sibs from zero survivor correction
** *****************************************************************************************

gen missing_sib = 0
append using "FILEPATH"
replace missing_sib = 1 if missing_sib == .
replace missing_sib = 1 if missing_sib == 2
replace country = iso3 if country == ""
** *****************************************************************************************
** 3. Create new sibship and PSU IDs
** *****************************************************************************************

gen newid = id+"-"+svy
egen id_sm = group(newid)                       

tostring surveyyear, gen(strsvyyr)
tostring v021, gen(strpsu) force
gen uniquepsu = country+"-"+strsvyyr+"-"+strpsu
egen psu = group(uniquepsu)

order country id_sm
sort country survey id_sm sibid

// Drop variables that are no longer needed 
drop id newid strpsu strsvyyr v021 uniquepsu v006
compress
replace yr_interview = yr_interview + 1900 if missing_sib != 1

compress
save "FILEPATH", replace 
