** VR deduplication of shocks non-war data
** date: 1/4/2017
** by:NAME
** purpose: There are disaster and injury shocks data that we get from VR.
** These need to be removed from the cause-specific VR file that is collapsed for
** all-cause mortality purposes.

** method: Sum deaths1 in VR by location-year-cause.

** set up and OS jes
clear all
set more off

	if c(os) == "Windows" {
		global j ""
	}
	else {
		global j ""
		set odbcmgr unixodbc
	}

** DEPENDENCIES
quietly{
	do "FILEPATH"
	do "FILEPATH"
}

** DIRECTORIES
global shockdir "FILEPATH"
global vrdir	"FILEPATH"
global outdir	"FILEPATH"

** FILENAMES
local disaster_VR_file "FILEPATH" // 
local war_VR_file		"FILEPATH"
local vrfile "VR_data_master_file_with_cause.dta"
local vrpath 		"$vrdir/`vrfile'"
local deduped_vr 	"VR_with_cause_shocks_deduplicated.dta"

*********************************************************************************************
** SCRIPT **

** bring in non-war shocks that originate in VR
use `disaster_VR_file', clear
rename cause acause
keep iso3 location_id year age_group_id sex acause nid numkilled
tempfile VR_already_in_shocks
save `VR_already_in_shocks', replace
** add the war VR data... all of it gets removed before Demographics VR prep
use `war_VR_file', clear
rename cause acause
rename war_deaths_best numkilled
keep iso3 location_id year age_group_id sex acause nid numkilled
append using `VR_already_in_shocks'
fastcollapse numkilled, type(sum) by(iso3 location_id year age_group_id sex nid)  // Aggregate all shocks
rename nid NID
save `VR_already_in_shocks', replace

* make iso3 -> loc_id map for non-subnational bearing countries in VR file
clear
get_location_metadata, location_set_id(35) gbd_round_id(4)
rename location_id location_id_to_add
keep ihme_loc_id location_id_to_add
rename ihme_loc_id iso3
tempfile iso_loc_table
save `iso_loc_table', replace


** Bring in VR file used for Demographics/Mortality -- change the vrfile as needed
use `vrpath', clear
fastcollapse deaths*, type(sum) by(iso3 location_id year sex source NID)  // Aggregate all causes
tempfile vr_full
save `vr_full', replace

merge m:1 iso3 using `iso_loc_table' // add location_id to the VR nationals without subnatonals; they need it later to be deduped.
replace location_id = location_id_to_add if location_id == .
assert location_id != .
keep if _m != 2
drop _m	location_id_to_add
tempfile vr_full_with_loc_id
save `vr_full_with_loc_id', replace

* fastcollapse deaths1, type(sum) by (iso3 location_id year acause) // aggregate to loc-year-cause level
//  03 April 2017: New method is to do this by age
reshape long deaths, i(iso3 location_id year sex source NID) j(who_age)
assert !inlist(who_age, 92, 4, 5, 6)
gen age_group_id = .
replace age_group_id = 2 if who_age == 91 // EN
replace age_group_id = 3 if who_age == 93 // LN
replace age_group_id = 4 if who_age == 94 // PN
replace age_group_id = 5 if who_age == 3	// 1-4
forvalues i = 6/20 {
	replace age_group_id = `i' if who_age == `i' + 1
}
replace age_group_id = 30 if who_age == 22	// 80-84
replace age_group_id = 31 if who_age == 23	// 85-89
replace age_group_id = 32 if who_age == 24	// 90-94
replace age_group_id = 235 if who_age == 25  // 95+
drop if age_group_id == .

merge 1:1 iso3 location_id year age_group_id sex NID using `VR_already_in_shocks', assert(1 3) keep(1 3)
replace deaths = deaths - numkilled if _m == 3
assert deaths >= 0 & deaths != .
drop numkilled _m age_group_id
reshape wide deaths, i(iso3 location_id year sex source NID) j(who_age)

saveold "$outdir/`deduped_vr'", replace
