// Purpose: Generate a list of countries and causes to move hiv deaths

if c(os) == "Unix" local dir "/home/j/WORK/03_cod/01_database/02_programs/hiv_correction/reallocation_program/inputs"
else if c(os) == "Windows" local dir "J:/WORK/03_cod/01_database/02_programs/hiv_correction/reallocation_program/inputs"

import excel using "`dir'/Master_Cause_Selections.xlsx", firstrow clear
keep iso3 cause
replace iso3 = trim(iso3)
replace cause = trim(cause)
saveold "`dir'/Master_Cause_Selections.dta", replace

