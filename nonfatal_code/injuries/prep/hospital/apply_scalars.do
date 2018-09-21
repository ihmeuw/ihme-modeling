// Author: USERNAME
// Date: DATE
// Purpose: Apply EN proportions scalars to cases by cause/iso3/year/age/sex

if c(os) == "Unix" {
	local j_prefix "FILEPATH"
	local h_prefix "FILEPATH"
	set odbcmgr unixodbc
}
else if c(os) == "Windows" {
	local j_prefix "FILEPATH"
	local h_prefix "FILEPATH"
}

set more off
clear all
adopath + "`j_prefix'/FILEPATH"

local repo = "`h_prefix'/FILEPATH"
local me_bundles = "FILEPATH.csv"

local scalar_dir "`j_prefix'/FILEPATH"
local hosp_dir "`j_prefix'/FILEPATH"

local hosp_version = "v6_2017_06_02"

// want to run these for the viz tool, or upload hospital data to database?
local viz_tool 1
local upload 1

// load modelable entities
import delimited "`repo'/`me_bundles'", delim(",") varn(1) clear
	keep if injury_metric == "Adjusted data"
	drop if e_code == "inj_war" | e_code == "inj_disaster"
	keep bundle_id e_code

	tempfile me_ids
	save `me_ids', replace

	// get e_codes to loop over
	levelsof e_code, l(ecodes) clean

// load EN proportions scalars and years to remove
import delimited "`scalar_dir'/FILEPATH.csv", delim(",") clear
	
	drop platform

	tempfile scalars
	save `scalars', replace

import delimited "`scalar_dir'/FILEPATH.csv", delim(",") clear

	drop platform
	tempfile scalars_NHAMCS
	save `scalars_NHAMCS', replace

// loop over raw hospital data, adjust with scalars, save for upload
if `viz_tool' {
	foreach ecode of local ecodes {
		di "Working on: `ecode'"

		use `me_ids' if e_code == "`ecode'", clear
			levelsof bundle_id, l(bundle)

		import excel "`hosp_dir'/FILEPATH.xlsx", firstrow clear
		replace measure = "incidence"

		gen cv_outpatient = 1 if regexm(source_type, "outpatient")
		replace cv_outpatient = 0 if regexm(source_type, "inpatient")

		merge m:1 location_id year_start year_end cv_outpatient using `scalars', assert(2 3) keep(3) nogen

		if "`ecode'" == "inj_medical" replace factor = 1

		rename mean_inj mean
		rename correction_factor_inj correction_factor
		rename upper_inj upper
		rename lower_inj lower

		gen mean_EN = mean * factor
		gen mean_EN_uncorrected = mean_0 * factor

		gen lower_EN = lower * factor
		gen lower_EN_uncorrected = lower_0 * factor

		gen upper_EN = upper * factor
		gen upper_EN_uncorrected = upper_0 * factor

		rename mean_0 mean_uncorrected
		rename lower_0 lower_uncorrected
		rename upper_0 upper_uncorrected

		drop if remove == 1
		keep measure location_id location_name year_start year_end age_start age_end sex mean* lower* upper* correction_factor factor cv_outpatient

		export delimited "`hosp_dir'/FILEPATH.csv", delim(",") replace
	}
}

// loop over raw hospital data and upload

if `upload' {
	foreach ecode of local ecodes {

		di "Working on: `ecode'"
		use `me_ids' if e_code == "`ecode'", clear
			levelsof bundle_id, l(bundle)

		import excel "`hosp_dir'/FILEPATH.xlsx", firstrow clear

		replace measure = "incidence"

		gen cv_outpatient = 1 if regexm(source_type, "outpatient")
		replace cv_outpatient = 0 if regexm(source_type, "inpatient")

		merge m:1 location_id year_start year_start cv_outpatient using `scalars', assert(2 3) keep(3) nogen

		if "`ecode'" == "inj_medical" replace factor = 1

		rename mean_inj mean
		rename upper_inj upper
		rename lower_inj lower

		replace mean = mean * factor
		replace lower = lower * factor
		replace upper = upper * factor

		replace upper = 0.0001 if upper == 0

		* need to drop the rows where it is all 0 because this is an age-restricted cause
		if inlist("`ecode'", "inj_suicide", "inj_suicide_firearm", "inj_suicide_other") {
			drop if mean == 0 & upper == 0 & lower == 0
		}

		drop if remove == 1
		drop remove

		* export the data and upload
		local filepath "`hosp_dir'/FILEPATH.csv"

		export excel "`filepath'", sheet("extraction") firstrow(var) replace

		upload_epi_data, bundle_id(`bundle') filepath(`filepath') clear
	}
}

// Now upload the outpatient data -----------------------------------------------

local nooutpat
local outpat
local nobundle

if `viz_tool' {
	foreach ecode of local ecodes {

		di "Working on: `ecode'"

		di "Working on: `ecode'"
		use `me_ids' if e_code == "`ecode'", clear
			levelsof bundle_id, l(bundle)

		cap import excel "`hosp_dir'/FILEPATH.xlsx", firstrow clear
		if _rc {
			cap import excel "`hosp_dir'/FILEPATH.xlsx", firstrow clear
			if _rc {
				local nooutpat `nooutpat' `ecode'
				local nobundle `nobundle' `bundle'
				continue
			}
		}
		replace measure = "incidence"

		gen cv_outpatient = 1 if regexm(source_type, "outpatient")
		replace cv_outpatient = 0 if regexm(source_type, "inpatient")

		merge m:1 location_id year_start cv_outpatient using `scalars_single', assert(2 3) keep(3) nogen

		if "`ecode'" == "inj_medical" replace factor = 1

		replace mean = mean * factor
		replace lower = lower * factor
		replace upper = upper * factor

		* need to drop the rows where it is all 0 because this is an age-restricted cause apparently
		if inlist("`ecode'", "inj_suicide", "inj_suicide_firearm", "inj_suicide_other") {
			drop if mean == 0 & upper == 0 & lower == 0
		}

		drop if remove == 1
		count
		if `r(N)' == 0 continue
		drop remove

		export delimited "`hosp_dir'/FILEPATH.csv", delim(",") replace
	}
}

// loop over raw hospital data and upload

if `upload' {
	foreach ecode of local ecodes {

		di "Working on: `ecode'"
		use `me_ids' if e_code == "`ecode'", clear
			levelsof bundle_id, l(bundle)

		cap import excel "`hosp_dir'/FILEPATH.xlsx", firstrow clear
		if _rc {
			cap import excel "`hosp_dir'/FILEPATH.xlsx", firstrow clear
			if _rc {
				local nooutpat `nooutpat' `ecode'
				local nobundle `nobundle' `bundle'
				continue
			}
		}
		replace measure = "incidence"

		gen cv_outpatient = 1 if regexm(source_type, "outpatient")
		replace cv_outpatient = 0 if regexm(source_type, "inpatient")

		merge m:1 location_id year_start cv_outpatient using `scalars_NHAMCS', assert(2 3) keep(3) nogen

		if "`ecode'" == "inj_medical" replace factor = 1

		replace cases = cases * factor

		drop if remove == 1
		count
		if `r(N)' == 0 continue
		drop remove

		* export the data and upload
		local filepath "`hosp_dir'/FILEPATH.csv"

		export excel "`filepath'", sheet("extraction") firstrow(var) replace

		upload_epi_data, bundle_id(`bundle') filepath(`filepath') clear
	}
}
