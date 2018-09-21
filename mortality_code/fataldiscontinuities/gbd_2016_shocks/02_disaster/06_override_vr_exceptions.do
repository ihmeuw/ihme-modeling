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
pause on

if c(os) == "Windows" {
	global prefix ""
	global shock_dir ""
}
else {
	global prefix ""
	set odbcmgr unixodbc
}

global datadir "FILEPATH"
do "FILEPATH"

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


*******************************************************************************
** IMPORTS
*******************************************************************************

// Import exceptions
//
import delimited using "FILEPATH", clear
keep location_id year cause numkilled_adj source nid ihme_loc_id
rename (source nid) (source_new nid_new)
tempfile adjustments
save `adjustments', replace

// Grab data before confidence intervals
use "FILEPATH", clear
// save original column names
ds
local vars = "`r(varlist)'"


*******************************************************************************
** IMPORTS
*******************************************************************************

merge m:1 location_id year cause using `adjustments'

// make sure that there is a clean replacement location-year-cause 
duplicates tag location_id year cause if _merge==3, gen(dups)
replace dups = 0 if dups == 1 & _merge == 3 & location_id == 16 // PHL := 16
assert dups == 0 if _merge == 3

** replace if there is an adjustment (merge 3)
** or totally new location-year-cause (merge 2)
replace numkilled = numkilled_adj if inlist(_merge, 2, 3)
replace source = source_new if inlist(_merge, 2, 3)
replace nid = nid_new if inlist(_merge, 2, 3)
replace iso3 = substr(ihme_loc_id, 1, 3) if inlist(_merge, 2, 3)
replace vr = 0 if inlist(_merge, 2, 3)
** this will be determined by the region
replace u_disaster_rate = . if inlist(_merge, 2, 3)
replace l_disaster_rate = . if inlist(_merge, 2, 3)

assert numkilled != .
assert iso3 != ""
assert location_id != .
assert year != .
assert cause != ""
** missing VR nids...?
replace nid = -9991 if source == "VR" & nid == .
replace nid = -9990 if source == "Gideon" | regexm(source, "WHO") | regexm(source, "Siddique")
// working on this NID as of 04/11/2016
assert nid != . if source != "NOAA"

** SDN/ SSD splitting ----------------------------------------
** replace the cause-years we know belong to SSD
** create new rows 
preserve
	keep if iso3 == "SDN" & ((year == 1996 & cause == "inj_disaster") | (year == 2008 & cause == "inj_trans_other"))
	expand 2, gen(orig)
	replace numkilled = numkilled - 100 if orig == 0 & (year == 1996 & cause == "inj_disaster")
	replace numkilled =  100 			if orig == 1 & (year == 1996 & cause == "inj_disaster")
	replace numkilled = numkilled - 24 	if orig == 0 & (year == 2008 & cause == "inj_trans_other")
	replace numkilled = 24 				if orig == 1 & (year == 2008 & cause == "inj_trans_other")
	tempfile sdn_ssd_splits
	save `sdn_ssd_splits', replace
restore
drop if iso3 == "SDN" & ((year == 1996 & cause == "inj_disaster") | (year == 2008 & cause == "inj_trans_other"))
append using `sdn_ssd_splits'

replace iso3 = "SSD" if iso3 == "SDN" & ((year == 1996 & cause == "inj_disaster") |	(year == 1998 & cause == "inj_fires")|(year == 2005 & cause == "inj_mech_other")|	(inlist(year, 1986, 2004) & cause == "inj_trans_other")| 	(year == 2002 & cause == "inj_trans_road_4wheel"))

// keep original variables and save
keep `vars'
saveold "FILEPATH", replace
saveold "FILEPATH", replace
