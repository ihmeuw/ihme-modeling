********************************************************
** Description:
** 
**
********************************************************

******************
** Set up Stata **
******************


cap program drop compile_45q15pop
program define compile_45q15pop

clear 
set mem 500m
set more off

*******************
** Define syntax **
*******************

syntax, iso3(string) saveas(string) 

************************
** Define directories **
************************

global rawdataDir = "FILEPATH"

global iso3_to_countryDir = "FILEPATH"
global iso3_to_countryFile = "FILEPATH"

global unDir = "FILEPATH"
global unFile = "FILEPATH"

global popDir = "FILEPATH"
global popFile = "FILEPATH"

global srsindDir = "FILEPATH"
global srsindFile = "FILEPATH"

global chndsp2004Dir = "FILEPATH"
global chndsp2004File = "FILEPATH"

global chndsp1995Dir = "FILEPATH" 
global chndsp1995File = "FILEPATH"

global chndsp9600Dir = "FILEPATH"
global chndsp9600File = "FILEPATH"

global censuspopforchnDir = "FILEPATH"
global censuspopforchnFile = "FILEPATH"

global chnmohDir = "FILEPATH"
global chnmohFile = "FILEPATH"

global chnmohrecentDir = "FILEPATH"
global chnmohrecentFile = "FILEPATH"

global chn1survey95Dir = "FILEPATH"
global chn1survey95File = "FILEPATH"

global chn1survey05Dir = "FILEPATH"
global chn1survey05File = "FILEPATH"

global twnhmdDir = "FILEPATH"
global twnhmdFile = "FILEPATH"


**global saveasDir "FILEPATH"
**global saveasFile "FILEPATH"

******************************
** Set up files for merging **
******************************

use "FILEPATH", clear
sort country
save, replace

*************
** UN Data **
*************

noisily: display in green "UN data"

insheet using "FILEPATH", clear names

g openend = .
forvalues j = 0/99 {
	local jplus = `j'+1
	replace openend = `jplus' if openend == . & pop_age`jplus' == .
}
replace openend = 100 if openend == .

replace pop_age80plus = . if openend == 100
rename pop_age80plus DATUM80plus
rename pop_age100 DATUM100plus 

rename pop_age0 DATUM0to0
egen DATUM1to4 = rowtotal(pop_age1-pop_age4)

forvalues j = 5(5)95 {
	local jplus = `j'+4
	egen DATUM`j'to`jplus' = rowtotal(pop_age`j'-pop_age`jplus')
}


egen DATUMTOT = rowtotal(DATUM*)
g DATUMUNK = .

drop pop_age* variant notes countrycode

rename sex SEX
rename year YEAR

preserve 
collapse (sum) DATUM*, by(country YEAR openend)
g SEX = 0
tempfile both
save `both', replace
restore

append using `both'

forvalues j = 80(5)95 {
	local jplus = `j'+4
	replace DATUM`j'to`jplus' = . if openend == 80
}
replace DATUM80plus = . if openend == 100
replace DATUM100plus = . if openend == 80

drop openend

sort country 
merge country using "FILEPATH"
tab _merge
keep if _merge == 3
drop _merge

drop country
rename iso3 COUNTRY

rename DATUM0to0 c1_0to0
rename DATUM1to4 c1_1to4
rename DATUM80plus c1_80plus
rename DATUM100plus c1_100plus

forvalues j = 5(5)95 {
	local jplus = `j'+4
	rename DATUM`j'to`jplus' c1_`j'to`jplus'
}

g c1_0to4 = .
g c1_85plus = .

forvalues j = 5(5)95 {
	local jplus = `j'+4
	replace c1_`j'to`jplus' = c1_`j'to`jplus'*1000
}

replace c1_0to0 = c1_0to0*1000
replace c1_1to4 = c1_1to4*1000
replace c1_80plus = c1_80plus*1000
replace c1_100plus = c1_100plus*1000

replace c1_0to4 = c1_0to0 + c1_1to4

drop DATUMTOT DATUMUNK

preserve
keep CO YEAR SEX
keep if CO == "CHN"
replace CO = "CHN_"
tempfile chndata_
save `chndata_', replace
restore

append using `chndata_'
replace CO = "CHNDYB" if CO == "CHN_"
append using `chndata_'
replace CO = "CHNWHO" if CO == "CHN_"
append using `chndata_'
replace CO = "CHNDSP" if CO == "CHN_" 
append using `chndata_'
replace CO = "CHNDC" if CO == "CHN_" 

sort CO YEAR SEX
tempfile main
save `main', replace

*****************
** Census Data **
*****************

noisily: display in green "Census data"

preserve
use "FILEPATH", clear
drop if agegroup2 ~= 2 & agegroup2 ~= 5 
g c1_0to0 = pop0 if agegroup2 == 2
egen c1_1to4 = rowtotal(pop1-pop4) if agegroup2 == 2
egen c1_0to4 = rowtotal(pop0-pop4)

g openend = .
forvalues j =  0/99 {
	local jplus = `j'+1
	replace openend = agegroup`j' if openend == . & agegroup`jplus' == .
}

forvalues j = 5(5)95 {
	local jplus = `j'+4
	egen c1_`j'to`jplus' = rowtotal(pop`j'-pop`jplus') if `j' < openend
}

forvalues j = 5/100 { 
	egen c1_`j'plus = rowtotal(pop`j'-pop100) if openend == `j'
	levelsof c1_`j'plus, local(dropit)
	if("`dropit'" == "") {
		drop c1_`j'plus
	}	
}

rename iso3 COUNTRY
rename year YEAR

g SEX = 0 if sex == "both"
replace SEX = 1 if sex == "male"
replace SEX = 2 if sex == "female"

drop sex

keep CO YEAR SEX c1_*

sort CO YEAR SEX

merge CO using `main'
keep if _merge == 1
drop _merge

sort CO YEAR SEX

tempfile censusdata
save `censusdata', replace
restore

append using `censusdata'


********************
** India SRS Data **
********************

noisily: display in green "India SRS data"

preserve
use "FILEPATH", clear

sort CO YEAR SEX
tempfile newdata
save `newdata', replace
restore

sort CO YEAR SEX
merge CO YEAR SEX using `newdata', keep(DATUM*)

replace c1_0to0 = . if _merge == 3
replace c1_1to4 = . if _merge == 3
replace c1_0to4 = DATUM0to4 if _merge == 3

forvalues j = 5(5)80 {
	local jplus = `j'+4
	replace c1_`j'to`jplus' = DATUM`j'to`jplus' if _merge == 3
}
replace c1_85plus = DATUM85plus if _merge == 3

drop DATUM* _merge

********************
** China DSP Data **
********************

noisily: display in green "China DSP data"

preserve
use "FILEPATH", clear
append using "FILEPATH"
append using "FILEPATH"
*use "FILEPATH", clear
*append using "FILEPATH"
*append using "FILEPATH"

replace CO = "CHNDSP"
sort CO YEAR SEX
tempfile newdata
save `newdata', replace
restore

sort CO YEAR SEX
merge CO YEAR SEX using `newdata', keep(DATUM*)

replace c1_0to0 = DATUM0to0 if _merge == 3
replace c1_1to4 = DATUM1to4 if _merge == 3
replace c1_0to4 = c1_0to0 + c1_0to4 if _merge == 3
forvalues j = 5(5)80 {
	local jplus = `j'+4
	replace c1_`j'to`jplus' = DATUM`j'to`jplus' if _merge == 3
}
replace c1_85plus = DATUM85plus if _merge == 3

drop DATUM* _merge

***********************
** China census Data **
***********************

noisily: display in green "China census data"

preserve
use "FILEPATH", clear
*use "FILEPATH", clear

rename iso3 COUNTRY
rename year YEAR
rename pop_source CENSUS_SOURCE

g SEX = 0 if sex == "both"
replace SEX = 1 if sex == "male"
replace SEX = 2 if sex == "female"

drop sex


keep if CO == "CHN" 
replace CO = "CHNDYB"
keep if strpos(CENSUS_SOURCE,"DYB") ~= 0
keep if YEAR == 1982 | YEAR == 1990 | YEAR == 2000
replace YEAR = YEAR - 1 if YEAR == 1990 | YEAR == 2000
sort DATUM0to0
duplicates drop CO YEAR SEX, force

sort CO YEAR SEX
tempfile newdata
save `newdata', replace
restore

sort CO YEAR SEX
merge CO YEAR SEX using `newdata', keep(DATUM*)
tab _merge

replace c1_0to0 = DATUM0to0 if _merge == 3
replace c1_1to4 = DATUM1to4 if _merge == 3
replace c1_0to4 = c1_0to0 + c1_0to4 if _merge == 3
forvalues j = 5(5)95 {
	local jplus = `j'+4
	replace c1_`j'to`jplus' = DATUM`j'to`jplus' if _merge == 3
}
replace c1_100plus = DATUM100plus if _merge == 3

drop DATUM* _merge

********************
** China MoH Data **
********************

noisily: display in green "China MoH data"

preserve
use "FILEPATH", clear
append using "FILEPATH"
*use "FILEPATH", clear
*append using "FILEPATH"
replace CO = "CHNWHO"
sort CO YEAR SEX
tempfile newdata
save `newdata', replace
restore

sort CO YEAR SEX
merge CO YEAR SEX using `newdata', keep(DATUM*)

replace c1_0to0 = DATUM0to0 if _merge == 3
replace c1_1to4 = DATUM1to4 if _merge == 3
replace c1_0to4 = c1_0to0 + c1_0to4 if _merge == 3
forvalues j = 5(5)80 {
	local jplus = `j'+4
	replace c1_`j'to`jplus' = DATUM`j'to`jplus' if _merge == 3
}
replace c1_85plus = DATUM85plus if _merge == 3

drop DATUM* _merge 

replace c1_0to4 = c1_0to0 + c1_1to4 if c1_0to4 == .

**************************
** China 1% Survey Data **
**************************

noisily: display in green "China 1% survey data"

preserve
use "FILEPATH", clear
append using "FILEPATH"
*use "FILEPATH", clear
*append using "FILEPATH"
replace CO = "CHNDC"
sort CO YEAR SEX
tempfile newdata
save `newdata', replace
restore

sort CO YEAR SEX
merge CO YEAR SEX using `newdata', keep(DATUM*)
tab _merge

replace c1_0to0 = DATUM0to0 if _merge == 3
replace c1_1to4 = DATUM1to4 if _merge == 3
replace c1_0to4 = c1_0to0 + c1_0to4 if _merge == 3
forvalues j = 5(5)95 {
	local jplus = `j'+4
	replace c1_`j'to`jplus' = DATUM`j'to`jplus' if _merge == 3
}
replace c1_100plus = DATUM100plus if _merge == 3

drop DATUM* _merge 

*********************
** Taiwan HMD data **
*********************

noisily: display in green "Taiwan HMD data"

sort CO YEAR SEX
*merge CO YEAR SEX using "FILEPATH", keep(DATUM*)
merge CO YEAR SEX using "FILEPATH", keep(DATUM*)
tab _merge

replace c1_0to0 = DATUM0to0 if _merge == 3 | _merge == 2
replace c1_1to4 = DATUM1to4 if _merge == 3 | _merge == 2
replace c1_0to4 = c1_0to0 + c1_0to4 if _merge == 3 | _merge == 2
forvalues j = 5(5)95 {
	local jplus = `j'+4
	replace c1_`j'to`jplus' = DATUM`j'to`jplus' if _merge == 3 | _merge == 2
}
replace c1_100plus = DATUM100to104 + DATUM105to109 + DATUM110plus if _merge == 3 | _merge == 2

drop DATUM* _merge 


replace c1_0to4 = c1_0to0 + c1_1to4 if c1_0to4 == .



** Should only be 7 points dropped
duplicates drop CO YEAR SEX, force

rename COUNTRY iso3
rename YEAR year
g sex = "both" if SEX == 0
replace sex = "male" if SEX == 1
replace sex = "female" if SEX == 2
drop SEX

*save "FILEPATH", replace

if("`iso3'" ~= "all" & "`iso3'" ~= "") {
	keep if strpos(iso3,"`iso3'") ~= 0
}

save "`saveas'", replace



end
