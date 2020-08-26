//
//  Purpose: Use the age pattern from metabolic mediators (BMI, FPG, SBP, Cholesterol) to determine percent change in RR from reference group (median age of exspoure) to each other age group
//            


// Compile FAO age trend results and apply them
clear all
macro drop _all
set maxvar 32000
// Set to run all selected code without pausing
set more off
// Remove previous restores
cap restore, not
// Define J drive (data) for cluster (UNIX) and Windows (Windows)
if c(os) == "Unix" {
	global j "/home/j"
	set odbcmgr unixodbc
}
else if c(os) == "Windows" {
	global j "J:"
}


local population 	"FILEPATH/pops_fao_as.dta"
local save_dir "/FILEPATH"

**************************************************************************************
********************* BMI - DM PREP **************************************************
**************************************************************************************
use "FILEPATH.dta", clear
egen rr_mean = rowmean(rr_*)
rename rr_mean pred
replace pred = log(pred)
drop rr_*
keep if year_id == 2015 & sex_id == 1
gen risk = "."

keep age_group_id pred risk
sort age_group_id

reshape wide pred, i(risk) j(age_group_id)

rename pred* pred_*

**prepare a separate trend for each of the different age at events needed for IHD
local event_ages 60 55
foreach age of local event_ages {
preserve
if `age' == 60 {
**prep this for being centered at 60 to correpsond to age at event = 60
	gen perc_21 = ((pred_21 - pred_17) / pred_17) + 1
	gen perc_20 = ((pred_20 - pred_17) / pred_17) + 1
	gen perc_19 = ((pred_19 - pred_17) / pred_17) + 1
	gen perc_18 = ((pred_18 - pred_17) / pred_17) + 1
	gen perc_17 = ((pred_17 - pred_17) / pred_17) + 1
	gen perc_16 = ((pred_16 - pred_17) / pred_17) + 1
	gen perc_15 = ((pred_15 - pred_17) / pred_17) + 1
	gen perc_14 = ((pred_14 - pred_17) / pred_17) + 1
	gen perc_13 = ((pred_13 - pred_17) / pred_17) + 1
	gen perc_12 = ((pred_12 - pred_17) / pred_17) + 1
	gen perc_11 = ((pred_11 - pred_17) / pred_17) + 1
	gen perc_10 = ((pred_10 - pred_17) / pred_17) + 1
}
if `age' == 55 {
**prep this for being centered at 55 to correpsond to age at event = 55
	gen perc_21 = ((pred_21 - pred_16) / pred_16) + 1
	gen perc_20 = ((pred_20 - pred_16) / pred_16) + 1
	gen perc_19 = ((pred_19 - pred_16) / pred_16) + 1
	gen perc_18 = ((pred_18 - pred_16) / pred_16) + 1
	gen perc_17 = ((pred_17 - pred_16) / pred_16) + 1
	gen perc_16 = ((pred_16 - pred_16) / pred_16) + 1
	gen perc_15 = ((pred_15 - pred_16) / pred_16) + 1
	gen perc_14 = ((pred_14 - pred_16) / pred_16) + 1
	gen perc_13 = ((pred_13 - pred_16) / pred_16) + 1
	gen perc_12 = ((pred_12 - pred_16) / pred_16) + 1
	gen perc_11 = ((pred_11 - pred_16) / pred_16) + 1
	gen perc_10 = ((pred_10 - pred_16) / pred_16) + 1
}


drop pred*

reshape long perc_, i(risk) 

rename _j age_group_id
rename perc_ percentage_change_bmi

//save the data
tempfile bmi_dm_`age'
save `bmi_dm_`age'', replace
restore
}
**************************************************************************************
**************************************************************************************
**************************************************************************************

**************************************************************************************
********************* BMI - IHD PREP *************************************************
**************************************************************************************
use "FILEPATH", clear
egen rr_mean = rowmean(rr_*)
rename rr_mean pred
replace pred = log(pred)
drop rr_*
keep if year_id == 2015 & sex_id == 1
gen risk = "."

keep age_group_id pred risk
sort age_group_id

reshape wide pred, i(risk) j(age_group_id)

rename pred* pred_*

**prepare a separate trend for each of the different age at events needed for IHD
local event_ages 60 55 65 50
foreach age of local event_ages {
preserve

if `age' == 60 {
**prep this for being centered at 60 to correpsond to age at event = 60
	gen perc_21 = ((pred_21 - pred_17) / pred_17) + 1
	gen perc_20 = ((pred_20 - pred_17) / pred_17) + 1
	gen perc_19 = ((pred_19 - pred_17) / pred_17) + 1
	gen perc_18 = ((pred_18 - pred_17) / pred_17) + 1
	gen perc_17 = ((pred_17 - pred_17) / pred_17) + 1
	gen perc_16 = ((pred_16 - pred_17) / pred_17) + 1
	gen perc_15 = ((pred_15 - pred_17) / pred_17) + 1
	gen perc_14 = ((pred_14 - pred_17) / pred_17) + 1
	gen perc_13 = ((pred_13 - pred_17) / pred_17) + 1
	gen perc_12 = ((pred_12 - pred_17) / pred_17) + 1
	gen perc_11 = ((pred_11 - pred_17) / pred_17) + 1
	gen perc_10 = ((pred_10 - pred_17) / pred_17) + 1
}
if `age' == 55 {
**prep this for being centered at 55 to correpsond to age at event = 55
	gen perc_21 = ((pred_21 - pred_16) / pred_16) + 1
	gen perc_20 = ((pred_20 - pred_16) / pred_16) + 1
	gen perc_19 = ((pred_19 - pred_16) / pred_16) + 1
	gen perc_18 = ((pred_18 - pred_16) / pred_16) + 1
	gen perc_17 = ((pred_17 - pred_16) / pred_16) + 1
	gen perc_16 = ((pred_16 - pred_16) / pred_16) + 1
	gen perc_15 = ((pred_15 - pred_16) / pred_16) + 1
	gen perc_14 = ((pred_14 - pred_16) / pred_16) + 1
	gen perc_13 = ((pred_13 - pred_16) / pred_16) + 1
	gen perc_12 = ((pred_12 - pred_16) / pred_16) + 1
	gen perc_11 = ((pred_11 - pred_16) / pred_16) + 1
	gen perc_10 = ((pred_10 - pred_16) / pred_16) + 1
}
if `age' == 65 {
**prep this for being centered at 65 to correpsond to age at event = 65
	gen perc_21 = ((pred_21 - pred_18) / pred_18) + 1
	gen perc_20 = ((pred_20 - pred_18) / pred_18) + 1
	gen perc_19 = ((pred_19 - pred_18) / pred_18) + 1
	gen perc_18 = ((pred_18 - pred_18) / pred_18) + 1
	gen perc_17 = ((pred_17 - pred_18) / pred_18) + 1
	gen perc_16 = ((pred_16 - pred_18) / pred_18) + 1
	gen perc_15 = ((pred_15 - pred_18) / pred_18) + 1
	gen perc_14 = ((pred_14 - pred_18) / pred_18) + 1
	gen perc_13 = ((pred_13 - pred_18) / pred_18) + 1
	gen perc_12 = ((pred_12 - pred_18) / pred_18) + 1
	gen perc_11 = ((pred_11 - pred_18) / pred_18) + 1
	gen perc_10 = ((pred_10 - pred_18) / pred_18) + 1
}
if `age' == 50 {
**prep this for being centered at 50 to correpsond to age at event = 50
	gen perc_21 = ((pred_21 - pred_15) / pred_15) + 1
	gen perc_20 = ((pred_20 - pred_15) / pred_15) + 1
	gen perc_19 = ((pred_19 - pred_15) / pred_15) + 1
	gen perc_18 = ((pred_18 - pred_15) / pred_15) + 1
	gen perc_17 = ((pred_17 - pred_15) / pred_15) + 1
	gen perc_16 = ((pred_16 - pred_15) / pred_15) + 1
	gen perc_15 = ((pred_15 - pred_15) / pred_15) + 1
	gen perc_14 = ((pred_14 - pred_15) / pred_15) + 1
	gen perc_13 = ((pred_13 - pred_15) / pred_15) + 1
	gen perc_12 = ((pred_12 - pred_15) / pred_15) + 1
	gen perc_11 = ((pred_11 - pred_15) / pred_15) + 1
	gen perc_10 = ((pred_10 - pred_15) / pred_15) + 1
}


drop pred*

reshape long perc_, i(risk) 

rename _j age_group_id
rename perc_ percentage_change_bmi

//save the data
tempfile bmi_ihd_`age'
save `bmi_ihd_`age'', replace
restore
}
**************************************************************************************
**************************************************************************************
**************************************************************************************

**************************************************************************************
********************* BMI - ISCH_STROKE PREP *****************************************
**************************************************************************************
use "FILEPATH", clear
egen rr_mean = rowmean(rr_*)
rename rr_mean pred
replace pred = log(pred)
drop rr_*
keep if year_id == 2015 & sex_id == 1
gen risk = "."

keep age_group_id pred risk
sort age_group_id

reshape wide pred, i(risk) j(age_group_id)

rename pred* pred_*

**prepare a separate trend for each of the different age at events needed for IHD
local event_ages 60 65
foreach age of local event_ages {
preserve

if `age' == 60 {
**prep this for being centered at 60 to correpsond to age at event = 60
	gen perc_21 = ((pred_21 - pred_17) / pred_17) + 1
	gen perc_20 = ((pred_20 - pred_17) / pred_17) + 1
	gen perc_19 = ((pred_19 - pred_17) / pred_17) + 1
	gen perc_18 = ((pred_18 - pred_17) / pred_17) + 1
	gen perc_17 = ((pred_17 - pred_17) / pred_17) + 1
	gen perc_16 = ((pred_16 - pred_17) / pred_17) + 1
	gen perc_15 = ((pred_15 - pred_17) / pred_17) + 1
	gen perc_14 = ((pred_14 - pred_17) / pred_17) + 1
	gen perc_13 = ((pred_13 - pred_17) / pred_17) + 1
	gen perc_12 = ((pred_12 - pred_17) / pred_17) + 1
	gen perc_11 = ((pred_11 - pred_17) / pred_17) + 1
	gen perc_10 = ((pred_10 - pred_17) / pred_17) + 1
}
if `age' == 65 {
**prep this for being centered at 65 to correpsond to age at event = 65
	gen perc_21 = ((pred_21 - pred_18) / pred_18) + 1
	gen perc_20 = ((pred_20 - pred_18) / pred_18) + 1
	gen perc_19 = ((pred_19 - pred_18) / pred_18) + 1
	gen perc_18 = ((pred_18 - pred_18) / pred_18) + 1
	gen perc_17 = ((pred_17 - pred_18) / pred_18) + 1
	gen perc_16 = ((pred_16 - pred_18) / pred_18) + 1
	gen perc_15 = ((pred_15 - pred_18) / pred_18) + 1
	gen perc_14 = ((pred_14 - pred_18) / pred_18) + 1
	gen perc_13 = ((pred_13 - pred_18) / pred_18) + 1
	gen perc_12 = ((pred_12 - pred_18) / pred_18) + 1
	gen perc_11 = ((pred_11 - pred_18) / pred_18) + 1
	gen perc_10 = ((pred_10 - pred_18) / pred_18) + 1
}


drop pred*

reshape long perc_, i(risk) 

rename _j age_group_id
rename perc_ percentage_change_bmi

//save the data
tempfile bmi_isch_stroke_`age'
save `bmi_isch_stroke_`age'', replace
restore
}
**************************************************************************************
**************************************************************************************
**************************************************************************************

**************************************************************************************
********************* BMI - HEM_STROKE PREP ******************************************
**************************************************************************************
use "FILEPATH", clear
egen rr_mean = rowmean(rr_*)
rename rr_mean pred
replace pred = log(pred)
drop rr_*
keep if year_id == 2015 & sex_id == 1
gen risk = "."

keep age_group_id pred risk
sort age_group_id

reshape wide pred, i(risk) j(age_group_id)

rename pred* pred_*

**prepare a separate trend for each of the different age at events needed for IHD
local event_ages 60 65
foreach age of local event_ages {
preserve

if `age' == 60 {
**prep this for being centered at 60 to correpsond to age at event = 60
	gen perc_21 = ((pred_21 - pred_17) / pred_17) + 1
	gen perc_20 = ((pred_20 - pred_17) / pred_17) + 1
	gen perc_19 = ((pred_19 - pred_17) / pred_17) + 1
	gen perc_18 = ((pred_18 - pred_17) / pred_17) + 1
	gen perc_17 = ((pred_17 - pred_17) / pred_17) + 1
	gen perc_16 = ((pred_16 - pred_17) / pred_17) + 1
	gen perc_15 = ((pred_15 - pred_17) / pred_17) + 1
	gen perc_14 = ((pred_14 - pred_17) / pred_17) + 1
	gen perc_13 = ((pred_13 - pred_17) / pred_17) + 1
	gen perc_12 = ((pred_12 - pred_17) / pred_17) + 1
	gen perc_11 = ((pred_11 - pred_17) / pred_17) + 1
	gen perc_10 = ((pred_10 - pred_17) / pred_17) + 1
}
if `age' == 65 {
**prep this for being centered at 65 to correpsond to age at event = 65
	gen perc_21 = ((pred_21 - pred_18) / pred_18) + 1
	gen perc_20 = ((pred_20 - pred_18) / pred_18) + 1
	gen perc_19 = ((pred_19 - pred_18) / pred_18) + 1
	gen perc_18 = ((pred_18 - pred_18) / pred_18) + 1
	gen perc_17 = ((pred_17 - pred_18) / pred_18) + 1
	gen perc_16 = ((pred_16 - pred_18) / pred_18) + 1
	gen perc_15 = ((pred_15 - pred_18) / pred_18) + 1
	gen perc_14 = ((pred_14 - pred_18) / pred_18) + 1
	gen perc_13 = ((pred_13 - pred_18) / pred_18) + 1
	gen perc_12 = ((pred_12 - pred_18) / pred_18) + 1
	gen perc_11 = ((pred_11 - pred_18) / pred_18) + 1
	gen perc_10 = ((pred_10 - pred_18) / pred_18) + 1
}


drop pred*

reshape long perc_, i(risk) 

rename _j age_group_id
rename perc_ percentage_change_bmi

//save the data
tempfile bmi_hem_stroke_`age'
save `bmi_hem_stroke_`age'', replace
restore
}
**************************************************************************************
**************************************************************************************
**************************************************************************************

**************************************************************************************
********************* SBP - IHD PREP *************************************************
**************************************************************************************
use "FILEPATH", clear
egen rr_mean = rowmean(rr_*)
rename rr_mean pred
replace pred = log(pred)
drop rr_*
keep if year_id == 2015 & sex_id == 1
gen risk = "."

keep age_group_id pred risk
sort age_group_id

reshape wide pred, i(risk) j(age_group_id)

rename pred* pred_*

**prepare a separate trend for each of the different age at events needed for IHD
local event_ages 60 55 65 50
foreach age of local event_ages {
preserve

if `age' == 60 {
**prep this for being centered at 60 to correpsond to age at event = 60
	gen perc_21 = ((pred_21 - pred_17) / pred_17) + 1
	gen perc_20 = ((pred_20 - pred_17) / pred_17) + 1
	gen perc_19 = ((pred_19 - pred_17) / pred_17) + 1
	gen perc_18 = ((pred_18 - pred_17) / pred_17) + 1
	gen perc_17 = ((pred_17 - pred_17) / pred_17) + 1
	gen perc_16 = ((pred_16 - pred_17) / pred_17) + 1
	gen perc_15 = ((pred_15 - pred_17) / pred_17) + 1
	gen perc_14 = ((pred_14 - pred_17) / pred_17) + 1
	gen perc_13 = ((pred_13 - pred_17) / pred_17) + 1
	gen perc_12 = ((pred_12 - pred_17) / pred_17) + 1
	gen perc_11 = ((pred_11 - pred_17) / pred_17) + 1
	gen perc_10 = ((pred_10 - pred_17) / pred_17) + 1
}
if `age' == 55 {
**prep this for being centered at 55 to correpsond to age at event = 55
	gen perc_21 = ((pred_21 - pred_16) / pred_16) + 1
	gen perc_20 = ((pred_20 - pred_16) / pred_16) + 1
	gen perc_19 = ((pred_19 - pred_16) / pred_16) + 1
	gen perc_18 = ((pred_18 - pred_16) / pred_16) + 1
	gen perc_17 = ((pred_17 - pred_16) / pred_16) + 1
	gen perc_16 = ((pred_16 - pred_16) / pred_16) + 1
	gen perc_15 = ((pred_15 - pred_16) / pred_16) + 1
	gen perc_14 = ((pred_14 - pred_16) / pred_16) + 1
	gen perc_13 = ((pred_13 - pred_16) / pred_16) + 1
	gen perc_12 = ((pred_12 - pred_16) / pred_16) + 1
	gen perc_11 = ((pred_11 - pred_16) / pred_16) + 1
	gen perc_10 = ((pred_10 - pred_16) / pred_16) + 1
}
if `age' == 65 {
**prep this for being centered at 65 to correpsond to age at event = 65
	gen perc_21 = ((pred_21 - pred_18) / pred_18) + 1
	gen perc_20 = ((pred_20 - pred_18) / pred_18) + 1
	gen perc_19 = ((pred_19 - pred_18) / pred_18) + 1
	gen perc_18 = ((pred_18 - pred_18) / pred_18) + 1
	gen perc_17 = ((pred_17 - pred_18) / pred_18) + 1
	gen perc_16 = ((pred_16 - pred_18) / pred_18) + 1
	gen perc_15 = ((pred_15 - pred_18) / pred_18) + 1
	gen perc_14 = ((pred_14 - pred_18) / pred_18) + 1
	gen perc_13 = ((pred_13 - pred_18) / pred_18) + 1
	gen perc_12 = ((pred_12 - pred_18) / pred_18) + 1
	gen perc_11 = ((pred_11 - pred_18) / pred_18) + 1
	gen perc_10 = ((pred_10 - pred_18) / pred_18) + 1
}
if `age' == 50 {
**prep this for being centered at 50 to correpsond to age at event = 50
	gen perc_21 = ((pred_21 - pred_15) / pred_15) + 1
	gen perc_20 = ((pred_20 - pred_15) / pred_15) + 1
	gen perc_19 = ((pred_19 - pred_15) / pred_15) + 1
	gen perc_18 = ((pred_18 - pred_15) / pred_15) + 1
	gen perc_17 = ((pred_17 - pred_15) / pred_15) + 1
	gen perc_16 = ((pred_16 - pred_15) / pred_15) + 1
	gen perc_15 = ((pred_15 - pred_15) / pred_15) + 1
	gen perc_14 = ((pred_14 - pred_15) / pred_15) + 1
	gen perc_13 = ((pred_13 - pred_15) / pred_15) + 1
	gen perc_12 = ((pred_12 - pred_15) / pred_15) + 1
	gen perc_11 = ((pred_11 - pred_15) / pred_15) + 1
	gen perc_10 = ((pred_10 - pred_15) / pred_15) + 1
}


drop pred*

reshape long perc_, i(risk) 

rename _j age_group_id
rename perc_ percentage_change_sbp

//save the data
tempfile sbp_ihd_`age'
save `sbp_ihd_`age'', replace
restore
}
**************************************************************************************
**************************************************************************************
**************************************************************************************

**************************************************************************************
********************* SBP - ISCH_STROKE PREP *****************************************
**************************************************************************************
use "FILEPATH", clear
egen rr_mean = rowmean(rr_*)
rename rr_mean pred
replace pred = log(pred)
drop rr_*
keep if year_id == 2015 & sex_id == 1
gen risk = "."

keep age_group_id pred risk
sort age_group_id

reshape wide pred, i(risk) j(age_group_id)

rename pred* pred_*

**prepare a separate trend for each of the different age at events needed for IHD
local event_ages 60 65
foreach age of local event_ages {
preserve

if `age' == 60 {
**prep this for being centered at 60 to correpsond to age at event = 60
	gen perc_21 = ((pred_21 - pred_17) / pred_17) + 1
	gen perc_20 = ((pred_20 - pred_17) / pred_17) + 1
	gen perc_19 = ((pred_19 - pred_17) / pred_17) + 1
	gen perc_18 = ((pred_18 - pred_17) / pred_17) + 1
	gen perc_17 = ((pred_17 - pred_17) / pred_17) + 1
	gen perc_16 = ((pred_16 - pred_17) / pred_17) + 1
	gen perc_15 = ((pred_15 - pred_17) / pred_17) + 1
	gen perc_14 = ((pred_14 - pred_17) / pred_17) + 1
	gen perc_13 = ((pred_13 - pred_17) / pred_17) + 1
	gen perc_12 = ((pred_12 - pred_17) / pred_17) + 1
	gen perc_11 = ((pred_11 - pred_17) / pred_17) + 1
	gen perc_10 = ((pred_10 - pred_17) / pred_17) + 1
}
if `age' == 65 {
**prep this for being centered at 65 to correpsond to age at event = 65
	gen perc_21 = ((pred_21 - pred_18) / pred_18) + 1
	gen perc_20 = ((pred_20 - pred_18) / pred_18) + 1
	gen perc_19 = ((pred_19 - pred_18) / pred_18) + 1
	gen perc_18 = ((pred_18 - pred_18) / pred_18) + 1
	gen perc_17 = ((pred_17 - pred_18) / pred_18) + 1
	gen perc_16 = ((pred_16 - pred_18) / pred_18) + 1
	gen perc_15 = ((pred_15 - pred_18) / pred_18) + 1
	gen perc_14 = ((pred_14 - pred_18) / pred_18) + 1
	gen perc_13 = ((pred_13 - pred_18) / pred_18) + 1
	gen perc_12 = ((pred_12 - pred_18) / pred_18) + 1
	gen perc_11 = ((pred_11 - pred_18) / pred_18) + 1
	gen perc_10 = ((pred_10 - pred_18) / pred_18) + 1
}


drop pred*

reshape long perc_, i(risk) 

rename _j age_group_id
rename perc_ percentage_change_sbp

//save the data
tempfile sbp_isch_stroke_`age'
save `sbp_isch_stroke_`age'', replace
restore
}
**************************************************************************************
**************************************************************************************
**************************************************************************************

**************************************************************************************
********************* SBP - HEM_STROKE PREP ******************************************
**************************************************************************************
use "FILEPATH", clear
egen rr_mean = rowmean(rr_*)
rename rr_mean pred
replace pred = log(pred)
drop rr_*
keep if year_id == 2015 & sex_id == 1
gen risk = "."

keep age_group_id pred risk
sort age_group_id

reshape wide pred, i(risk) j(age_group_id)

rename pred* pred_*

**prepare a separate trend for each of the different age at events needed for IHD
local event_ages 60 65
foreach age of local event_ages {
preserve

if `age' == 60 {
**prep this for being centered at 60 to correpsond to age at event = 60
	gen perc_21 = ((pred_21 - pred_17) / pred_17) + 1
	gen perc_20 = ((pred_20 - pred_17) / pred_17) + 1
	gen perc_19 = ((pred_19 - pred_17) / pred_17) + 1
	gen perc_18 = ((pred_18 - pred_17) / pred_17) + 1
	gen perc_17 = ((pred_17 - pred_17) / pred_17) + 1
	gen perc_16 = ((pred_16 - pred_17) / pred_17) + 1
	gen perc_15 = ((pred_15 - pred_17) / pred_17) + 1
	gen perc_14 = ((pred_14 - pred_17) / pred_17) + 1
	gen perc_13 = ((pred_13 - pred_17) / pred_17) + 1
	gen perc_12 = ((pred_12 - pred_17) / pred_17) + 1
	gen perc_11 = ((pred_11 - pred_17) / pred_17) + 1
	gen perc_10 = ((pred_10 - pred_17) / pred_17) + 1
}
if `age' == 65 {
**prep this for being centered at 65 to correpsond to age at event = 65
	gen perc_21 = ((pred_21 - pred_18) / pred_18) + 1
	gen perc_20 = ((pred_20 - pred_18) / pred_18) + 1
	gen perc_19 = ((pred_19 - pred_18) / pred_18) + 1
	gen perc_18 = ((pred_18 - pred_18) / pred_18) + 1
	gen perc_17 = ((pred_17 - pred_18) / pred_18) + 1
	gen perc_16 = ((pred_16 - pred_18) / pred_18) + 1
	gen perc_15 = ((pred_15 - pred_18) / pred_18) + 1
	gen perc_14 = ((pred_14 - pred_18) / pred_18) + 1
	gen perc_13 = ((pred_13 - pred_18) / pred_18) + 1
	gen perc_12 = ((pred_12 - pred_18) / pred_18) + 1
	gen perc_11 = ((pred_11 - pred_18) / pred_18) + 1
	gen perc_10 = ((pred_10 - pred_18) / pred_18) + 1
}


drop pred*

reshape long perc_, i(risk) 

rename _j age_group_id
rename perc_ percentage_change_sbp

//save the data
tempfile sbp_hem_stroke_`age'
save `sbp_hem_stroke_`age'', replace
restore
}
**************************************************************************************
**************************************************************************************
**************************************************************************************

**************************************************************************************
********************* FPG - IHD PREP *************************************************
**************************************************************************************
use "FILEPATH", clear
egen rr_mean = rowmean(rr_*)
rename rr_mean pred
replace pred = log(pred)
drop rr_*
keep if year_id == 2015 & sex_id == 1
gen risk = "."

keep age_group_id pred risk
sort age_group_id

reshape wide pred, i(risk) j(age_group_id)

rename pred* pred_*

**prepare a separate trend for each of the different age at events needed for IHD
local event_ages 60 55 65 50
foreach age of local event_ages {
preserve

if `age' == 60 {
**prep this for being centered at 60 to correpsond to age at event = 60
	gen perc_21 = ((pred_21 - pred_17) / pred_17) + 1
	gen perc_20 = ((pred_20 - pred_17) / pred_17) + 1
	gen perc_19 = ((pred_19 - pred_17) / pred_17) + 1
	gen perc_18 = ((pred_18 - pred_17) / pred_17) + 1
	gen perc_17 = ((pred_17 - pred_17) / pred_17) + 1
	gen perc_16 = ((pred_16 - pred_17) / pred_17) + 1
	gen perc_15 = ((pred_15 - pred_17) / pred_17) + 1
	gen perc_14 = ((pred_14 - pred_17) / pred_17) + 1
	gen perc_13 = ((pred_13 - pred_17) / pred_17) + 1
	gen perc_12 = ((pred_12 - pred_17) / pred_17) + 1
	gen perc_11 = ((pred_11 - pred_17) / pred_17) + 1
	gen perc_10 = ((pred_10 - pred_17) / pred_17) + 1
}
if `age' == 55 {
**prep this for being centered at 55 to correpsond to age at event = 55
	gen perc_21 = ((pred_21 - pred_16) / pred_16) + 1
	gen perc_20 = ((pred_20 - pred_16) / pred_16) + 1
	gen perc_19 = ((pred_19 - pred_16) / pred_16) + 1
	gen perc_18 = ((pred_18 - pred_16) / pred_16) + 1
	gen perc_17 = ((pred_17 - pred_16) / pred_16) + 1
	gen perc_16 = ((pred_16 - pred_16) / pred_16) + 1
	gen perc_15 = ((pred_15 - pred_16) / pred_16) + 1
	gen perc_14 = ((pred_14 - pred_16) / pred_16) + 1
	gen perc_13 = ((pred_13 - pred_16) / pred_16) + 1
	gen perc_12 = ((pred_12 - pred_16) / pred_16) + 1
	gen perc_11 = ((pred_11 - pred_16) / pred_16) + 1
	gen perc_10 = ((pred_10 - pred_16) / pred_16) + 1
}
if `age' == 65 {
**prep this for being centered at 65 to correpsond to age at event = 65
	gen perc_21 = ((pred_21 - pred_18) / pred_18) + 1
	gen perc_20 = ((pred_20 - pred_18) / pred_18) + 1
	gen perc_19 = ((pred_19 - pred_18) / pred_18) + 1
	gen perc_18 = ((pred_18 - pred_18) / pred_18) + 1
	gen perc_17 = ((pred_17 - pred_18) / pred_18) + 1
	gen perc_16 = ((pred_16 - pred_18) / pred_18) + 1
	gen perc_15 = ((pred_15 - pred_18) / pred_18) + 1
	gen perc_14 = ((pred_14 - pred_18) / pred_18) + 1
	gen perc_13 = ((pred_13 - pred_18) / pred_18) + 1
	gen perc_12 = ((pred_12 - pred_18) / pred_18) + 1
	gen perc_11 = ((pred_11 - pred_18) / pred_18) + 1
	gen perc_10 = ((pred_10 - pred_18) / pred_18) + 1
}
if `age' == 50 {
**prep this for being centered at 50 to correpsond to age at event = 50
	gen perc_21 = ((pred_21 - pred_15) / pred_15) + 1
	gen perc_20 = ((pred_20 - pred_15) / pred_15) + 1
	gen perc_19 = ((pred_19 - pred_15) / pred_15) + 1
	gen perc_18 = ((pred_18 - pred_15) / pred_15) + 1
	gen perc_17 = ((pred_17 - pred_15) / pred_15) + 1
	gen perc_16 = ((pred_16 - pred_15) / pred_15) + 1
	gen perc_15 = ((pred_15 - pred_15) / pred_15) + 1
	gen perc_14 = ((pred_14 - pred_15) / pred_15) + 1
	gen perc_13 = ((pred_13 - pred_15) / pred_15) + 1
	gen perc_12 = ((pred_12 - pred_15) / pred_15) + 1
	gen perc_11 = ((pred_11 - pred_15) / pred_15) + 1
	gen perc_10 = ((pred_10 - pred_15) / pred_15) + 1
}


drop pred*

reshape long perc_, i(risk) 

rename _j age_group_id
rename perc_ percentage_change_fpg

//save the data
tempfile fpg_ihd_`age'
save `fpg_ihd_`age'', replace
restore
}
**************************************************************************************
**************************************************************************************
**************************************************************************************

**************************************************************************************
********************* FPG - ISCH_STROKE PREP *****************************************
**************************************************************************************
use "FILEPATH", clear
egen rr_mean = rowmean(rr_*)
rename rr_mean pred
replace pred = log(pred)
drop rr_*
keep if year_id == 2015 & sex_id == 1
gen risk = "."

keep age_group_id pred risk
sort age_group_id

reshape wide pred, i(risk) j(age_group_id)

rename pred* pred_*

**prepare a separate trend for each of the different age at events needed for IHD
local event_ages 60 65
foreach age of local event_ages {
preserve

if `age' == 60 {
**prep this for being centered at 60 to correpsond to age at event = 60
	gen perc_21 = ((pred_21 - pred_17) / pred_17) + 1
	gen perc_20 = ((pred_20 - pred_17) / pred_17) + 1
	gen perc_19 = ((pred_19 - pred_17) / pred_17) + 1
	gen perc_18 = ((pred_18 - pred_17) / pred_17) + 1
	gen perc_17 = ((pred_17 - pred_17) / pred_17) + 1
	gen perc_16 = ((pred_16 - pred_17) / pred_17) + 1
	gen perc_15 = ((pred_15 - pred_17) / pred_17) + 1
	gen perc_14 = ((pred_14 - pred_17) / pred_17) + 1
	gen perc_13 = ((pred_13 - pred_17) / pred_17) + 1
	gen perc_12 = ((pred_12 - pred_17) / pred_17) + 1
	gen perc_11 = ((pred_11 - pred_17) / pred_17) + 1
	gen perc_10 = ((pred_10 - pred_17) / pred_17) + 1
}
if `age' == 65 {
**prep this for being centered at 65 to correpsond to age at event = 65
	gen perc_21 = ((pred_21 - pred_18) / pred_18) + 1
	gen perc_20 = ((pred_20 - pred_18) / pred_18) + 1
	gen perc_19 = ((pred_19 - pred_18) / pred_18) + 1
	gen perc_18 = ((pred_18 - pred_18) / pred_18) + 1
	gen perc_17 = ((pred_17 - pred_18) / pred_18) + 1
	gen perc_16 = ((pred_16 - pred_18) / pred_18) + 1
	gen perc_15 = ((pred_15 - pred_18) / pred_18) + 1
	gen perc_14 = ((pred_14 - pred_18) / pred_18) + 1
	gen perc_13 = ((pred_13 - pred_18) / pred_18) + 1
	gen perc_12 = ((pred_12 - pred_18) / pred_18) + 1
	gen perc_11 = ((pred_11 - pred_18) / pred_18) + 1
	gen perc_10 = ((pred_10 - pred_18) / pred_18) + 1
}


drop pred*

reshape long perc_, i(risk) 

rename _j age_group_id
rename perc_ percentage_change_fpg

//save the data
tempfile fpg_isch_stroke_`age'
save `fpg_isch_stroke_`age'', replace
restore
}
**************************************************************************************
**************************************************************************************
**************************************************************************************

**************************************************************************************
********************* FPG - HEM_STROKE PREP ******************************************
**************************************************************************************
use "FILEPATH", clear
egen rr_mean = rowmean(rr_*)
rename rr_mean pred
replace pred = log(pred)
drop rr_*
keep if year_id == 2015 & sex_id == 1
gen risk = "."

keep age_group_id pred risk
sort age_group_id

reshape wide pred, i(risk) j(age_group_id)

rename pred* pred_*

**prepare a separate trend for each of the different age at events needed for HEM STROKE
local event_ages 60 65
foreach age of local event_ages {
preserve

if `age' == 60 {
**prep this for being centered at 60 to correpsond to age at event = 60
	gen perc_21 = ((pred_21 - pred_17) / pred_17) + 1
	gen perc_20 = ((pred_20 - pred_17) / pred_17) + 1
	gen perc_19 = ((pred_19 - pred_17) / pred_17) + 1
	gen perc_18 = ((pred_18 - pred_17) / pred_17) + 1
	gen perc_17 = ((pred_17 - pred_17) / pred_17) + 1
	gen perc_16 = ((pred_16 - pred_17) / pred_17) + 1
	gen perc_15 = ((pred_15 - pred_17) / pred_17) + 1
	gen perc_14 = ((pred_14 - pred_17) / pred_17) + 1
	gen perc_13 = ((pred_13 - pred_17) / pred_17) + 1
	gen perc_12 = ((pred_12 - pred_17) / pred_17) + 1
	gen perc_11 = ((pred_11 - pred_17) / pred_17) + 1
	gen perc_10 = ((pred_10 - pred_17) / pred_17) + 1
}
if `age' == 65 {
**prep this for being centered at 65 to correpsond to age at event = 65
	gen perc_21 = ((pred_21 - pred_18) / pred_18) + 1
	gen perc_20 = ((pred_20 - pred_18) / pred_18) + 1
	gen perc_19 = ((pred_19 - pred_18) / pred_18) + 1
	gen perc_18 = ((pred_18 - pred_18) / pred_18) + 1
	gen perc_17 = ((pred_17 - pred_18) / pred_18) + 1
	gen perc_16 = ((pred_16 - pred_18) / pred_18) + 1
	gen perc_15 = ((pred_15 - pred_18) / pred_18) + 1
	gen perc_14 = ((pred_14 - pred_18) / pred_18) + 1
	gen perc_13 = ((pred_13 - pred_18) / pred_18) + 1
	gen perc_12 = ((pred_12 - pred_18) / pred_18) + 1
	gen perc_11 = ((pred_11 - pred_18) / pred_18) + 1
	gen perc_10 = ((pred_10 - pred_18) / pred_18) + 1
}


drop pred*

reshape long perc_, i(risk) 

rename _j age_group_id
rename perc_ percentage_change_fpg

//save the data
tempfile fpg_hem_stroke_`age'
save `fpg_hem_stroke_`age'', replace
restore
}
**************************************************************************************
**************************************************************************************
**************************************************************************************

**************************************************************************************
********************* CHOLESTEROL - IHD PREP *****************************************
**************************************************************************************
use "FILEPATH", clear
egen rr_mean = rowmean(rr_*)
rename rr_mean pred
replace pred = log(pred)
drop rr_*
keep if year_id == 2015 & sex_id == 1
gen risk = "."

keep age_group_id pred risk
sort age_group_id

reshape wide pred, i(risk) j(age_group_id)

rename pred* pred_*

**prepare a separate trend for each of the different age at events needed for IHD
local event_ages 60 55 65 50
foreach age of local event_ages {
preserve

if `age' == 60 {
**prep this for being centered at 60 to correpsond to age at event = 60
	gen perc_21 = ((pred_21 - pred_17) / pred_17) + 1
	gen perc_20 = ((pred_20 - pred_17) / pred_17) + 1
	gen perc_19 = ((pred_19 - pred_17) / pred_17) + 1
	gen perc_18 = ((pred_18 - pred_17) / pred_17) + 1
	gen perc_17 = ((pred_17 - pred_17) / pred_17) + 1
	gen perc_16 = ((pred_16 - pred_17) / pred_17) + 1
	gen perc_15 = ((pred_15 - pred_17) / pred_17) + 1
	gen perc_14 = ((pred_14 - pred_17) / pred_17) + 1
	gen perc_13 = ((pred_13 - pred_17) / pred_17) + 1
	gen perc_12 = ((pred_12 - pred_17) / pred_17) + 1
	gen perc_11 = ((pred_11 - pred_17) / pred_17) + 1
	gen perc_10 = ((pred_10 - pred_17) / pred_17) + 1
}
if `age' == 55 {
**prep this for being centered at 55 to correpsond to age at event = 55
	gen perc_21 = ((pred_21 - pred_16) / pred_16) + 1
	gen perc_20 = ((pred_20 - pred_16) / pred_16) + 1
	gen perc_19 = ((pred_19 - pred_16) / pred_16) + 1
	gen perc_18 = ((pred_18 - pred_16) / pred_16) + 1
	gen perc_17 = ((pred_17 - pred_16) / pred_16) + 1
	gen perc_16 = ((pred_16 - pred_16) / pred_16) + 1
	gen perc_15 = ((pred_15 - pred_16) / pred_16) + 1
	gen perc_14 = ((pred_14 - pred_16) / pred_16) + 1
	gen perc_13 = ((pred_13 - pred_16) / pred_16) + 1
	gen perc_12 = ((pred_12 - pred_16) / pred_16) + 1
	gen perc_11 = ((pred_11 - pred_16) / pred_16) + 1
	gen perc_10 = ((pred_10 - pred_16) / pred_16) + 1
}
if `age' == 65 {
**prep this for being centered at 65 to correpsond to age at event = 65
	gen perc_21 = ((pred_21 - pred_18) / pred_18) + 1
	gen perc_20 = ((pred_20 - pred_18) / pred_18) + 1
	gen perc_19 = ((pred_19 - pred_18) / pred_18) + 1
	gen perc_18 = ((pred_18 - pred_18) / pred_18) + 1
	gen perc_17 = ((pred_17 - pred_18) / pred_18) + 1
	gen perc_16 = ((pred_16 - pred_18) / pred_18) + 1
	gen perc_15 = ((pred_15 - pred_18) / pred_18) + 1
	gen perc_14 = ((pred_14 - pred_18) / pred_18) + 1
	gen perc_13 = ((pred_13 - pred_18) / pred_18) + 1
	gen perc_12 = ((pred_12 - pred_18) / pred_18) + 1
	gen perc_11 = ((pred_11 - pred_18) / pred_18) + 1
	gen perc_10 = ((pred_10 - pred_18) / pred_18) + 1
}
if `age' == 50 {
**prep this for being centered at 50 to correpsond to age at event = 50
	gen perc_21 = ((pred_21 - pred_15) / pred_15) + 1
	gen perc_20 = ((pred_20 - pred_15) / pred_15) + 1
	gen perc_19 = ((pred_19 - pred_15) / pred_15) + 1
	gen perc_18 = ((pred_18 - pred_15) / pred_15) + 1
	gen perc_17 = ((pred_17 - pred_15) / pred_15) + 1
	gen perc_16 = ((pred_16 - pred_15) / pred_15) + 1
	gen perc_15 = ((pred_15 - pred_15) / pred_15) + 1
	gen perc_14 = ((pred_14 - pred_15) / pred_15) + 1
	gen perc_13 = ((pred_13 - pred_15) / pred_15) + 1
	gen perc_12 = ((pred_12 - pred_15) / pred_15) + 1
	gen perc_11 = ((pred_11 - pred_15) / pred_15) + 1
	gen perc_10 = ((pred_10 - pred_15) / pred_15) + 1
}


drop pred*

reshape long perc_, i(risk) 

rename _j age_group_id
rename perc_ percentage_change_cholesterol

//save the data
tempfile cholesterol_ihd_`age'
save `cholesterol_ihd_`age'', replace
restore
}
**************************************************************************************
**************************************************************************************
**************************************************************************************

**************************************************************************************
********************* CHOLESTEROL - ISCH_STROKE PREP *********************************
**************************************************************************************
use "FILEPATH", clear
egen rr_mean = rowmean(rr_*)
rename rr_mean pred
replace pred = log(pred)
drop rr_*
keep if year_id == 2015 & sex_id == 1
gen risk = "."

keep age_group_id pred risk
sort age_group_id

reshape wide pred, i(risk) j(age_group_id)

rename pred* pred_*

**prepare a separate trend for each of the different age at events needed for IHD
local event_ages 60 65
foreach age of local event_ages {
preserve

if `age' == 60 {
**prep this for being centered at 60 to correpsond to age at event = 60
	gen perc_21 = ((pred_21 - pred_17) / pred_17) + 1
	gen perc_20 = ((pred_20 - pred_17) / pred_17) + 1
	gen perc_19 = ((pred_19 - pred_17) / pred_17) + 1
	gen perc_18 = ((pred_18 - pred_17) / pred_17) + 1
	gen perc_17 = ((pred_17 - pred_17) / pred_17) + 1
	gen perc_16 = ((pred_16 - pred_17) / pred_17) + 1
	gen perc_15 = ((pred_15 - pred_17) / pred_17) + 1
	gen perc_14 = ((pred_14 - pred_17) / pred_17) + 1
	gen perc_13 = ((pred_13 - pred_17) / pred_17) + 1
	gen perc_12 = ((pred_12 - pred_17) / pred_17) + 1
	gen perc_11 = ((pred_11 - pred_17) / pred_17) + 1
	gen perc_10 = ((pred_10 - pred_17) / pred_17) + 1
}


drop pred*

reshape long perc_, i(risk) 

rename _j age_group_id
rename perc_ percentage_change_cholesterol

//save the data
tempfile cholesterol_isch_stroke_`age'
save `cholesterol_isch_stroke_`age'', replace
restore
}

**************************************************************************************
**************************************************************************************
**************************************************************************************


**************************************************************************************
********************* Prepare those that will be averaged ****************************
**************************************************************************************
**Averaging for  ihd
use `sbp_ihd_60', clear
	merge 1:1 age_group_id using `bmi_ihd_60', nogen
	merge 1:1 age_group_id using `fpg_ihd_60', nogen
	merge 1:1 age_group_id using `cholesterol_ihd_60', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		save "`FILEPATH/cvd_ihd.dta", replace
		tempfile diet_fruit_cvd_ihd
		save `diet_fruit_cvd_ihd', replace
		**save `set_1', replace

**Averaging for isch_stroke
use `sbp_isch_stroke_60', clear
	merge 1:1 age_group_id using `bmi_isch_stroke_60', nogen
	merge 1:1 age_group_id using `fpg_isch_stroke_60', nogen
	merge 1:1 age_group_id using `cholesterol_isch_stroke_60', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		save "FILEPATH/cvd_stroke_isch.dta", replace
		tempfile diet_fruit_cvd_stroke_isch
		save `diet_fruit_cvd_stroke_isch', replace

**Averaging for hem_stroke
use `sbp_hem_stroke_60', clear
	merge 1:1 age_group_id using `bmi_hem_stroke_60', nogen
	merge 1:1 age_group_id using `fpg_hem_stroke_60', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		save "FILEPATH/cvd_stroke_cerhem.dta", replace
		tempfile diet_fruit_cvd_stroke_cerhem
		save `diet_fruit_cvd_stroke_cerhem', replace

**Averaging for diabetes
use `bmi_dm_60', clear
	gen percentage_change = percentage_change_bmi
		keep risk age_group_id percentage_change
		save "`FILEPATH/diabetes.dta", replace
		tempfile diet_fruit_diabetes
		save `diet_fruit_diabetes', replace

