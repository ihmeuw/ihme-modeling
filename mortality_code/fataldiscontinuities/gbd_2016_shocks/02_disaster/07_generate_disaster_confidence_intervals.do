// NAME
// June 21, 2012
// Modified Dec 11 2015 by NAME to use cause
// Purpose: generate confidence intervals for type-specific disaster deaths



*******************************************************************************
** SET-UP
*******************************************************************************

clear all
set more off
pause on

if c(os) == "Windows" {
	global prefix ""
}
else {
	global prefix ""
	set odbcmgr unixodbc
}


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

// bring in confidence intervals (created in war/code/11_combine_all_death_sources.do)
use "FILEPATH", clear
keep region_id low_per_diff high_per_diff
duplicates drop
tempfile confidence
save `confidence', replace


// Population for 1970 - 2015
//local dsn prodcod
//odbc load, exec("SELECT year_id, location_id, sex_id, age_group_id, mean_pop, age_group_name_short FROM mortality.output JOIN mortality.output_version USING (output_version_id) JOIN shared.age_group USING (age_group_id) WHERE is_best = 1") dsn(`dsn') clear
//keep if sex_id == 3
//keep if age_group_id == 22
//keep year_id location_id mean_pop
//rename year_id year
use "FILEPATH", clear
keep if sex_id == 3
keep if age_group_id >= 2 & age_group_id <= 21
rename year_id year

collapse (sum) pop, by(location_id year) fast
rename pop mean_pop

tempfile population
save `population', replace

// GBD region names
//odbc load, exec("SELECT location_id, region_name FROM shared.location_hierarchy_history WHERE location_set_version_id = 46") clear dsn(prodcod)
//drop if region_name == ""
//duplicates drop
//rename region_name gbd_region
get_location_metadata, location_set_id(35) gbd_round_id(4) clear
keep location_id region_id region_name
tempfile regions
save `regions', replace

// Get compiled and prioritized data with exceptions implemented
use "FILEPATH", clear

*******************************************************************************
** GENERATE CONFIDENCE INTERVALS AND CHANGE TO RATE-SPACE
*******************************************************************************

merge m:1 location_id using `regions'
	drop if _m == 1 
	assert _m != 1
	keep if _m == 3
	drop _m

merge m:1 year location_id using `population'
	assert _m != 1 if year >= 1950
	keep if _m == 3
	drop _m

rename numkilled disaster
rename mean_pop pop

// merge in confidence intervals dataset
merge m:1 region_id using `confidence'
	assert _m != 1
	drop if _m != 3
	drop _m
// _m =2: don't have disaster data
//drop if _m == 2

// Australia data: don't trust it, so use world mean CI for that one; otherwise, use the regional CI
egen temp_low = mean(low_per_diff)
egen temp_high = mean(high_per_diff)
//replace low_per_diff = temp_low if region_id == "Australasia"
//replace high_per_diff = temp_high if region_id == "Australasia"

drop temp*

// add in the regional CIs for countries that weren't in the UCDP CI file
sort region_id low_per_diff
carryforward low_per_diff, replace
sort region_id high_per_diff
carryforward high_per_diff, replace
assert low_per_diff != .
assert high_per_diff != . 

// disaster rate
gen double disaster_rate = disaster/pop

assert low_per_diff <= high_per_diff

replace l_disaster_rate = disaster_rate*low_per_diff if l_disaster_rate == .
replace u_disaster_rate = disaster_rate*high_per_diff if u_disaster_rate == .

// if the l_disaster rate is infinitesimally greater than disaster_rate (happened once), make it equal
gen lminb = l_disaster_rate-disaster_rate
replace l_disaster_rate= disaster_rate if (lminb<.00000001) & (lminb>0)
drop lminb

// if the u_disaster_rate is infinitesimally smaller than the disaster_rate (one occurance), make them equal
gen umaxb = u_disaster_rate - disaster_rate
replace u_disaster_rate = disaster_rate if (abs(umaxb) < 0.0000001) & (umaxb < 0)
drop umaxb
	replace u_disaster_rate = disaster_rate if year == 2016 & iso3 == "ITA"
// make sure that disaster rate is between the lower and upper bounds
assert disaster_rate >= l_disaster_rate
assert disaster_rate <= u_disaster_rate

//drop low_per_diff high_per_diff country* ihme_indic_country
drop low_per_diff high_per_diff

compress
// get average of duplicated country-years across EMDAT and the online supplement
di in red "If it's no longer 62 deaths, then compare it to the online supplement and determine if we still need to use the supplement or if EMDAT is more reasonable now"
//assert disaster == 62 if iso3 == "PHL" & year == 2013 & source == "EMDAT" 
drop if iso3 == "PHL" & year == 2013 & source == "Online supplement" & disaster == 6437	

drop dup //  
duplicates tag iso3 location_id year cause, gen(dup)
// Keep high of EMDAT vs Online supplement dups
local vtype : type disaster
bysort iso3 location_id year cause : egen `vtype' max = max(disaster)
drop if disaster != max & dup > 0
drop if disaster == max & dup > 0 & source != "EMDAT"	// Prefer EMDAT over online supplement if deaths are the same

//foreach ratevar of varlist disaster_rate l_disaster_rate u_disaster_rate {
//	bysort iso3 year: egen mean_`ratevar' = mean(`ratevar')
//	replace `ratevar' = mean_`ratevar' if dup != 0
//}
//duplicates drop iso3 location_id year cause, force
drop dup
	
// final formatting
//drop location_id

// save in our folder
saveold "FILEPATH", replace
saveold "FILEPATH", replace

