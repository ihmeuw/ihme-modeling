adopath + FILEPATH

foreach period in green pink red {
	tempfile `period'Merge
	forvalues i = 1 / 1000 {
		import delimited using "FILEPATH\prop_`i'.csv", clear
		drop v1
		if `i'>1 quietly merge 1:1 zone using ``period'Merge', assert(3) nogenerate
		save ``period'Merge', replace
		}
	generate period = "`period'"
	save ``period'Merge', replace
	}

clear
append using `greenMerge' `pinkMerge' `redMerge'

rename prop_1000 prop_0
rename zone location_id

order prop_*, sequential  after(period)

* get species specific exclusions and only keep the locations we want
preserve
import delimited "FILEPATH/species_specific_exclusions.csv", clear varn(1)
keep if japonicum == ""
levelsof location_id, local(jap_locs)
* add in laos and Cambodia (for mekongi)
local jap_locs = "`jap_locs' 10 12"
restore

gen keep = 0
foreach loc of local jap_locs {
	replace keep = 1 if location_id == `loc'
}
keep if keep == 1
drop keep
tempfile combined
save `combined'

* get non-china japonicum/other locations
keep if period == "green" & !inlist(location_id, 491,494,496,497,503,504,506,507,514,516,520,521)
keep location_id prop_*
tempfile japonicum
save `japonicum', replace
/*keep if period == "red"
keep location_id prop_*
tempfile japonicum
save `japonicum', replace
levelsof location_id, local(jap_locs)*/

** now pull in the mansoni/haematobium proportions at risk
clear
tempfile schisto
forvalues i = 1/1000 {
	import delimited using "FILEPATH/prop_`i'.csv", clear
	drop v1
	if `i'>1 quietly merge 1:1 zone using `schisto', assert(3) nogenerate
	save `schisto', replace
}
rename prop_1000 prop_0
rename zone location_id
* drop japonicum locations
foreach loc of local jap_locs {
	drop if location_id == `loc'
}
* append japonicum stuff (this is just for 2017)
append using `japonicum'
tempfile schisto_all
save `schisto_all', replace

* merge on location information
get_location_metadata, location_set_id(35) clear
keep location_id location_name ihme_loc_id
merge 1:1 location_id using `schisto_all', assert(1 3) keep(3) nogenerate

egen mean = rowmean(prop*)



capture mkdir FILEPATH
save "FILEPATH/prop_draws_urbanmask.dta", replace
export delimited "FILEPATH/prop_draws_urbanmask.csv", replace

use `combined', clear
save "FILEPATH\combined_urbanmask.dta", replace
use "FILEPATH\combined_urbanmask.dta", clear

* just keep china
keep if inlist(location_id, 491,494,496,497,503,504,506,507,514,516,520,521)

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

get_covariate_estimates, covariate_id(881) clear
rename mean_value sdi
keep location_id year_id sdi

merge 1:1 location_id year_id using `data', assert(1 3) keep(3) nogenerate



order location_id period year_id propMean
sort  location_id year_id


gen lnMean = ln(propModel)
mixed lnMean sdi || location_id: sdi
predict pred, fitted
replace pred = exp(pred)

levelsof location_id if year_id==1990 & propModel==0, local(zeroLocations) sep(,) clean
replace pred = 0 if inlist(location_id, `zeroLocations') & year_id>=1990

levelsof location_id if year_id==2017 & propModel==0, local(zeroLocations) sep(,) clean
**replace pred = 0 if inlist(location_id, `zeroLocations') & year_id>=2017

forvalues i = 0 / 999 {
	quietly replace prop_`i' = prop_`i' * pred / propMean
	quietly replace prop_`i' = 0 if propMean==0 | pred==0
	}

keep if (mod(year_id, 5)==0 & year_id!=2015 & year_id>=1990) | year_id==2017
keep location_id year_id prop_*

*save "FILEPATH/prop_draws_chn.dta", replace
save "FILEPATH/prop_draws_chn_urbanmask.dta", replace
