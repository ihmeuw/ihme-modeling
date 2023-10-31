### NTDs Dengue

*Simplifies code execution if these are saved as .dta files  

import delimited "FILEPATH/pop_age20.csv", clear

save "FILEPATH/pop_age20.dta", replace

clear

import delimited "FILEPATH/dengue_cases.csv", clear

save "FILEPATH/dengue_cases.dta", replace
