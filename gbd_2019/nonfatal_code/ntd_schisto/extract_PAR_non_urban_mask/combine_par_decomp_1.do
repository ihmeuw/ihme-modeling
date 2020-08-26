** this pulls in most recent BRT results and combines them together

** prep stata
clear all
set more off
set maxvar 32000
if c(os) == "Unix" {
	global prefix "ADDRESS"
	set odbcmgr ADDRESS
}
else if c(os) == "Windows" {
	global prefix "ADDRESS"
}


** set adopath
adopath + $prefix/FILEPATH 
run $prefix/FILEPATH



import delimited using "FILEPATH"
****GBD 2019 Decomp Step 1 code***


use "$prefix/FILEPATH", replace
export delimited using "FILEPATH", replace

	tempfile merge
	forvalues i = 1 / 1000 {
		import delimited using "FILEPATH", clear
		drop v1
		if `i'>1 quietly merge 1:1 zone using `merge', assert(3) nogenerate
		save `merge', replace
	}


save "FILEPATH", replace
use "FILEPATH", clear


	
rename prop_1000 prop_0
rename zone location_id

order prop_*, sequential  

tempfile combined
save `combined'

keep if !inlist(location_id, 491,494,496,497,503,504,506,507,514,516,520,521)
keep location_id prop_*

save "FILEPATH", replace

export delimited using "FILEPATH", replace

import delimited using "FILEPATH", clear
save "FILEPATH", replace
save "$prefix/FILEPATH", replace


get_location_metadata, location_set_id(35) clear
keep location_id location_name ihme_loc_id

merge 1:m location_id using `combined', assert(1 3) keep(3) nogenerate


save "new name", replace

use "$prefix/FILEPATH", clear

use "$prefix/FILEPATH", replace





	*****************
**GBD 2017 CODE****	
	local period "red"
	tempfile `period'Merge
	forvalues i = 1 / 1000 {
		import delimited using "$prefix/FILEPATH", clear
		drop v1
		if `i'>1 quietly merge 1:1 zone using ``period'Merge', assert(3) nogenerate
		save ``period'Merge', replace
	}

	generate period = "`period'"
	save ``period'Merge', replace

clear

local period "pink"
	tempfile `period'Merge
	forvalues i = 2 / 1000 {
		import delimited using "$prefix/FILEPATH", clear
		drop v1
		if `i'>2 quietly merge 1:1 zone using ``period'Merge', assert(3) nogenerate
		save ``period'Merge', replace
	}

	generate period = "`period'"
	save ``period'Merge', replace


clear 

local period "green"
	tempfile `period'Merge
	forvalues i = 1 / 1000 {
		import delimited using "$prefix/FILEPATH", clear
		drop v1
		if `i'>1 quietly merge 1:1 zone using ``period'Merge', assert(3) nogenerate
		save ``period'Merge', replace
	}

	generate period = "`period'"
	save ``period'Merge', replace

clear
append using `greenMerge' `pinkMerge' `redMerge'


rename prop_1000 prop_0
rename zone location_id

order prop_*, sequential  after(period)

tempfile combined
save `combined'

keep if period == "green" & !inlist(location_id, 491,494,496,497,503,504,506,507,514,516,520,521)
keep location_id prop_*
capture mkdir $prefix/FILEPATH
save "$prefix/FILEPATH", replace

get_location_metadata, location_set_id(35) clear
keep location_id location_name ihme_loc_id

merge 1:m location_id using `combined', assert(1 3) keep(3) nogenerate

use "$prefix/FILEPATH", clear

save "$prefix/FILEPATH", replace
use "$prefix/FILEPATH", clear




keep if strmatch(ihme_loc_id, "CHN_*")
gen year_id = 1980 if period=="green"
replace year_id = 1990 if period=="pink"
replace year_id = 2017 if period=="red"

egen propMean = rowmean(prop_*)
expand 39 if year_id==1980, gen(newObs)
bysort location_id newObs: replace year_id = _n + 1979 if newObs==1
	
drop if inlist(year_id, 1980, 1990, 2017) & newObs==1
generate propModel = propMean if inlist(year_id, 1980, 1990, 2017)

tempfile data
save `data'


get_covariate_estimates, covariate_id(ADDRESS) decomp_step("iterative") clear

rename mean_value sdi
keep location_id year_id sdi

merge 1:1 location_id year_id using `data', assert(1 3) keep(3) nogenerate


order location_id location_name ihme_loc_id period year_id propMean
sort  location_id year_id


gen lnMean = ln(propModel)
mixed lnMean sdi || location_id: sdi
predict pred, fitted
replace pred = exp(pred)

levelsof location_id if year_id==1990 & propModel==0, local(zeroLocations) sep(,) clean
replace pred = 0 if inlist(location_id, `zeroLocations') & year_id>=1990

levelsof location_id if year_id==2017 & propModel==0, local(zeroLocations) sep(,) clean

replace pred = 0 if inlist(location_id, `zeroLocations') & year_id>=2017

forvalues i = 0 / 999 {
	quietly replace prop_`i' = prop_`i' * pred / propMean
	quietly replace prop_`i' = 0 if propMean==0 | pred==0
	}

keep if (mod(year_id, 5)==0 & year_id!=2015 & year_id>=1990) | year_id==2017
keep location_id year_id prop_*

save "$prefix/FILEPATH", replace
