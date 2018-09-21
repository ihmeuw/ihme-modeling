// Author: NAME 
// Date: 2016/04/11
// Purpose: Sometimes high-quality VR is chosen over EMDAT but it is cUSERy wrong. We think this is likely due to the
// impact of the disaster on the VR system. For example, in Colombia in 1995 there was a volcano explosion with a 
// consensus impact of around 20,000 deaths, but this does not show up in the VR system at all.


*******************************************************************************
** SET-UP
*******************************************************************************

clear all
set more off

if c(os) == "Windows" {
	global prefix "PATH"
}
else {
	global prefix "/FILEPATH"
	set odbcmgr unixodbc
}

global datadir "FILENAME"


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


** database setup
do "FILENAME"
create_connection_string, server("modeling-cod-db") database("cod")
local conn_string `r(conn_string)'

*******************************************************************************
** IMPORTS
*******************************************************************************

// Import exceptions
// 


** combination of IMS and IBC used for iraq 2003 - 2016
import delimited using "FILEPATH", clear
tempfile iraq_ims_ibc
save `iraq_ims_ibc', replace

import delimited using "FILEPATH", clear 
drop if ihme_loc_id == "IRQ" & year > 2002 & year < 2017
append using `iraq_ims_ibc'

keep ihme_loc_id year cause deathnumberbest low high source nid
collapse (sum) deathnumberbest, by(ihme_loc_id year cause low high source nid) fast
rename (source nid) (source_new nid_new)
isid ihme_loc_id year

// drop the us and gbr for 2014-2015 that are pretty small and dont match with VR
drop if inlist(ihme_loc_id, "GBR", "USA") & deathnumberbest < 70 & inlist(year, 2014, 2015)


replace cause = "war"

tempfile adjustments
save `adjustments', replace

// Grab data before confidence intervals
use "$datadir/war_compiled_prioritized.dta", clear
// save original column names
ds
local vars = "`r(varlist)'"


*******************************************************************************
** IMPORTS
*******************************************************************************

merge m:1 ihme_loc_id year cause using `adjustments'
drop if _m == 2
assert inlist(_m, 1, 3)
// now, there are ofen multiple entries for an ihme_loc_id-year-cause
// this merge just determined what to drop from the data

drop if _merge==3
append using `adjustments'
// I'll call _merge = 4 "new data"
replace _merge=4 if _merge==.

** replace if there is an adjustment (merge 3)
** or totally new location-year-cause (merge 2)
replace war_deaths_best = deathnumberbest if _merge==4
replace war_deaths_low = low if _merge==4
replace war_deaths_high = high if _merge==4
replace source = source_new if _merge==4
replace nid = nid_new if _merge==4
replace sex = "both" if _merge==4

assert war_deaths_best != .
assert war_deaths_low <= war_deaths_best if war_deaths_low != .
assert war_deaths_best <= war_deaths_high if war_deaths_high != . & war_deaths_high != 0
assert ihme_loc_id != ""
assert year != .
assert cause != ""
assert sex != ""
assert nid != .
assert source != ""

isid ihme_loc_id year cause nid

** get rid of mexico cartels ( this is the UCDP non-state dataset)
drop if nid==231049 & year>=2008 & regexm(ihme_loc_id, "MEX")

** Genocides ***************
** Rwanda 1994 **
replace cause = "war" if inlist(ihme_loc_id, "RWA", "BDI") & year == 1994 & cause == "legal intervention"
replace cause = "war" if ihme_loc_id == "COD" & 1994 <= year & year < 1999 & cause == "legal intervention"
replace cause = "war" if cause == "legal intervention" & war_deaths_best > 1000

// keep original variables and save
keep `vars'
saveold "$datadir/war_with_exceptions.dta", replace
saveold "$datadir/archive/war_with_exceptions_`timestamp'.dta", replace
