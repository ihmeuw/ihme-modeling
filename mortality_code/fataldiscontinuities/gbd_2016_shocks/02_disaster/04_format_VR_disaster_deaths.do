** ****************************************************
** Author:NAME 
** Modified: NAME, 30 Dec 2015 to compile only inj_disaster & inj_war deaths from VR
** Purpose: Prepare and compile all final Cause of Death sources for mortality team
** Date Created: 3/21/2014
** Location: do "FILEPATH"
** Location: do "FILEPATH"
** ****************************************************

** ***************************************************************************************************************************** **
** set up stata
clear all
set more off
set rmsg on
set mem 4g
if c(os) == "Windows" {
	global j ""
}
if c(os) == "Unix" {
	global j ""
	set odbcmgr unixodbc
}

** ***************************************************************************************************************************** **
quietly {
	do "FILEPATH"
	do "FILEPATH"
	do "FILEPATH"
	do "FILEPATH"
}
	create_connection_string, server("modeling-cod-db")
	local conn_string `r(conn_string)'

** define globals
global vrdir "FILEPATH"
global date = c(current_date)

get_location_metadata, location_set_id(35) clear
keep if most_detailed == 1
keep location_id ihme_loc_id
gen iso3 = substr(ihme_loc_id,1,3)
drop ihme_loc_id
tempfile isos
save `isos', replace

******
** NAME 31 Mar 2017 - we will pull technologial shocks from dB (so we can have redistributed data), then war and disaster from file
******
// First, get tech shocks from database
clear
gen location_id = .
tempfile db_data
save `db_data'
local techcids 699 707 700 695 387  // 693 -- MOTOR VEHICLE NO LONGER TAKEN OUT OF VR AS SCHOCK
foreach cid of local techcids {
	di "Fetching cause_id `cid'"
	quietly {
		get_cod_data, cause_id("`cid'") is_outlier("1") clear
		tempfile outliers
		save `outliers', replace
		get_cod_data, cause_id("`cid'") is_outlier("0") clear
		append using `outliers', force
		erase `outliers'
		drop if inlist(age_group_id, 22, 27) | sex == 3
		keep if data_type == "Vital Registration"
		duplicates drop
		rename study_deaths numkilled
		rename acause cause
		fastcollapse numkilled, type(sum) by(location_id year age_group_id sex cause nid)
		append using `db_data'
		save `db_data', replace
	}
}

// Attach iso3
merge m:1 location_id using `isos', keep(3) nogen

** final prep and store to be added on to shocks from file
gen source = "VR"
order iso3 location_id year age_group_id sex cause source nid numkilled
sort iso3 location_id year age_group_id sex cause source nid numkilled
save `db_data', replace

// Next, get shock causes from file
use "FILEPATH", clear
keep if inlist(acause, "inj_disaster", "inj_war", "inj_war_war", "inj_war_terrorism") // , "inj_war_execution" -- NO LONGER A SHOCK CAUSE (per NAME, 05 April 2017)
rename NID nid
reshape long deaths, i(iso3 location_id year sex acause source nid) j(who_age)
gen age_group_id = .
replace age_group_id = 2 if inlist(who_age, 91, 92) // EN
replace age_group_id = 3 if who_age == 93 // LN
replace age_group_id = 4 if who_age == 94 // PN
replace age_group_id = 5 if inlist(who_age, 3, 4, 5, 6)	// 1-4
forvalues i = 6/20 {
	replace age_group_id = `i' if who_age == `i' + 1
}
replace age_group_id = 30 if who_age == 22	// 80-84
replace age_group_id = 31 if who_age == 23	// 85-89
replace age_group_id = 32 if who_age == 24	// 90-94
replace age_group_id = 235 if who_age == 25  // 95+
drop if age_group_id == .
rename acause cause
rename deaths numkilled
fastcollapse numkilled, type(sum) by(iso3 location_id year age_group_id sex cause source nid)
preserve
	clear
	get_location_metadata, location_set_id(35) gbd_round_id(4)
	rename location_id location_id_2	// need this to check if a subnational already has a location_id
	keep ihme_loc_id location_id_2
	rename ihme_loc_id iso3
	tempfile iso_loc_table
	save `iso_loc_table', replace
restore
merge m:1 iso3 using `iso_loc_table' // , update keep(1 3 4 5) nogen // add loc_ids to non-subnatl countries
replace location_id = location_id_2 if location_id == .
assert location_id != .
assert _m == 2 if year == .
drop if _m == 2 // drops 645 rows where we have no non-warterror VR shock deaths
drop location_id_2 _merge
append using `db_data'  // Add in the technological shocks
replace source = "VR"

drop if iso3 == "PSE" & year == 2004 & nid != 132705
drop if iso3 == "PSE" & year == 2005 & nid != 132706
drop if iso3 == "PSE" & year == 2006 & nid != 132708 
drop if iso3 == "PSE" & year == 2007 & nid != 132708
isid location_id year age_group_id sex cause

preserve
	keep if !regexm(cause, "inj_war")
	order iso3 location_id year age_group_id sex cause source numkilled
	sort iso3 location_id year age_group_id sex cause source numkilled
	compress
	saveold "FILEPATH", replace
	fastcollapse numkilled, type(sum) by(iso3 location_id year cause source nid)  // Collapse to location-year-cause
	saveold "FILEPATH", replace
restore
	keep if inlist(cause, "inj_war", "inj_war_war", "inj_war_terrorism")  // , "inj_war_execution"
	gen dataset_ind = 99
	rename numkilled war_deaths_best
	order iso3 location_id year age_group_id sex cause source dataset_ind war_deaths_best
	sort iso3 location_id year age_group_id sex cause source war_deaths_best
	compress
	saveold "FILEPATH", replace
	fastcollapse war_deaths_best, type(sum) by(iso3 location_id year cause source nid)  // Collapse to location-year-cause
	saveold "FILEPATH", replace
