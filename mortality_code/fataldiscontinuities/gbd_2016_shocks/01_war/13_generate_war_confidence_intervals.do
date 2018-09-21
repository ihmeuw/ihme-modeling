// NAME 
// June 21, 2012
// Modified Dec 11 2015 by  NAME to use cause
// Purpose: generate confidence intervals for type-specific disaster deaths

// CHANGE THE DATE AT THE END WITHIN THE "SAVE" LINE -- we don't want it in the way that the local creates it, we want it the way the data team likes it


*******************************************************************************
** SET-UP
*******************************************************************************

clear all
set more off

if c(os) == "Windows" {
	global prefix ""
}
else {
	global prefix ""
	set odbcmgr unixodbc
}

// worth separating these if the out dir is ever different from the in dir
global datadir "FILEPATH"
global outdir "FILEPATH"

// Set the timestamp
local date = c(current_date)
local date = c(current_date)
local today = date("`date'", "DMY")
local year = year(`today')
local month = month(`today')
local day = day(`today')
local time = c(current_time)
local time : subinstr local time ":" "", all
local length : length local month
if `length' == 1 local month = "0`month'"	
local length : length local day
if `length' == 1 local day = "0`day'"
local date = "`year'_`month'_`day'"
local timestamp = "`date'_`time'"

run "FILEPATH"
create_connection_string
local conn_string `r(conn_string)'

run "FILEPATH"

*******************************************************************************
** IMPORTS
*******************************************************************************

// BRING IN POP
use "FILEPATH", clear
// for compatitbility with old code
rename year_id year 
keep if age_group_id >= 2 & age_group_id <= 21
keep if sex_id == 3

// collapse pop to ihme_loc_id year
collapse (sum) pop, by(ihme_loc_id year region_id) fast
rename pop tot
tempfile pop
save `pop', replace

// COUNTRY CODES
get_location_metadata, location_set_id(35) clear
keep ihme_loc_id location_id region_id region_name location_ascii_name
rename location_ascii_name location_name
	
tempfile countrycodes 
save `countrycodes', replace

use "$datadir/war_with_exceptions.dta", clear

*******************************************************************************
** FILL IN CONFIDENCE INTERVALS
*******************************************************************************
	
merge m:1 ihme_loc_id year using `pop'
// there are 36 iso3-location_id pairs that are not real
	drop if _m == 1 
	assert _m != 1
	drop if _m != 3
	drop _merge

** CI fix 04/09/2016
** these didnt actually have confidence intervals and collapse makes the high estimate 0
replace war_deaths_high = . if war_deaths_best!=0 & war_deaths_high==0
** now, any low with a war_deaths_high that is missing didnt have a confidence interval either
replace war_deaths_low = . if war_deaths_high==. 

gen best=war_deaths_best/tot
gen lower=war_deaths_low/tot
gen upper=war_deaths_high/tot

**********************************
// making confidence intervals using death rates
// calculate percent difference between low-best and high-best to use a confidence intervals for the disaster dataset
gen low_difference = (best - lower)/best
gen high_difference = (best - upper)/best

replace low_diff = 1-low_diff
replace high_diff = 1-high_diff
replace high_diff = 1 if high_diff < 1 & high_diff > .9

// aggregate to the region level
merge m:1 ihme_loc_id using `countrycodes'
drop if ihme_loc_id == "VIR" & _m == 1
// 12/1/16: dropping 3 provinces in Kenya
drop if _m == 1 
assert _m != 1
drop if _m != 3
drop _m

// only keeping UCDP -- this is the dataset we want to base all confidence intervals on, at regional level
// furthermore, we only want the national-level isos to create the confidence intervals
preserve
keep if inlist(source, "UCDP", "combined sources for Arab countries")
//drop if gbd_country_iso3 != "" & iso3 != "MAC" & iso3 != "HKG" & iso3 != "CHN" & iso3 != "IND" & iso3 != "GBR" & iso3 != "MEX"
sort region_id
by region_id: egen low_per_diff = mean(low_diff)
by region_id: egen high_per_diff = mean(high_diff)
keep region_id low_per_diff high_per_diff

duplicates drop

tempfile region
save `region', replace
restore

merge m:1 region_id using `region'
// Everything in data should match except maybe AUS & NZL, which are in their own region
assert inlist(ihme_loc_id, "AUS", "NZL") if _m == 1
drop if _m == 2
drop _m
// Use Western Europe for AUS & NZL
foreach diff in low_per_diff high_per_diff {
	levelsof `diff' if region_id == 73, local(value) clean
	replace `diff' = `value' if region_id == 70
}


**************************************************
// applying confidence intervals to other data sources
// using the results from 06, generate confidence intervals for IISS

replace lower = best*low_per_diff if source != "UCDP" 
replace upper = best*high_per_diff if source != "UCDP"

assert best >= lower if source != "UCDP"
assert best <= upper if source != "UCDP"


replace lower = best*low_per_diff if source == "UCDP" & best==lower
replace upper = best*high_per_diff if source == "UCDP" & best==upper
replace upper = best*high_per_diff if source == "UCDP" & upper == .

assert upper != .
assert lower != .
	
assert best >= lower
assert best <= upper

** replace war_deaths_low with low rate*tot now that missing low_rates
** have been filled in (first make sure that tot is still correct)
gen best_diff = abs((best*tot)-war_deaths_best)
su best_diff
** difference should be simply floating point stuff
assert `r(max)'<.05
** now do it
replace war_deaths_low = lower*tot
replace war_deaths_high = upper*tot
assert war_deaths_low<=war_deaths_best
assert war_deaths_best<=war_deaths_high

// save CI dataset for disaster
preserve
keep ihme_loc_id region_id low_per_diff high_per_diff
duplicates drop
tempfile ci
save `ci', replace
* because it's at the regional level, we need ci's for all countries: just use the regional one for that country
merge 1:1 ihme_loc_id region_id using `countrycodes'
	assert _m != 1
	drop if _m==2
	drop _m
	
//drop if gbd_country_iso3 != "" & iso3 != "MAC" & iso3 != "HKG" & iso3 != "CHN" & iso3 != "IND" & iso3 != "GBR" & iso3 != "MEX"
drop if inlist(ihme_loc_id, "BRA", "CHN", "GBR", "IDN", "IND", "JPN", "KEN", "MEX") | inlist(ihme_loc_id, "SAU", "SWE", "USA", "ZAF")
sort region_id ihme_loc_id
foreach ci_var of varlist low_per_diff high_per_diff {
	carryforward `ci_var', replace
}
	
saveold "$datadir/UCDP_ci_for_disaster_Africa.dta", replace
saveold "$datadir/UCDP_ci_for_disaster_`date'.dta", replace
restore

// finalize dataset
rename best war_rate
rename lower l_war_rate
rename upper u_war_rate
rename ihme_loc_id iso3
keep year iso3 location_id cause war_deaths_low war_deaths_high war_deaths_best sex tot u_war_rate l_war_rate war_rate source nid
	

// confirming that we have one source for every iso3 location_id year 
//isid iso3 year cause
isid iso3 location_id year cause nid

** GBD 2016 3/31/17 : no war in Okinawa in 1972, a VR mistake.
drop if regexm(iso3, "JPN") & year == 1979 & regexm(cause, "war") & source == "VR"
** US deaths 2003 on are too high, only use DoD data
drop if regexm(iso3, "USA") & year == 2003 & regexm(cause, "war")
** SDN war deaths in 70s should be in SSD (population splitting would give too many to the north...)
replace iso3 = "SSD" if iso3 == "SDN" & year > 1970 & year < 1980 & regexm(cause, "war")
** KHM genocide use our CIs
replace war_deaths_low = 140000 if source == "KHM Genocide"
replace war_deaths_high = 600000 if source == "KHM Genocide"

compress
saveold "$datadir/USABLE_EST_GLOBAL_vWARDEATHS.dta", replace
saveold "$datadir/archive/USABLE_EST_GLOBAL_vWARDEATHS_`timestamp'.dta", replace
