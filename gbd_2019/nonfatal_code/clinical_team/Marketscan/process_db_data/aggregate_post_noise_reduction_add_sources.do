// submit prevalence extraction jobs

if c(os) == "Unix" {
	local prefix "FILENAME"
	global prefix "FILENAME"
	local K "FILENAME"
	global K "FILENAME"
	set more off
	set odbcmgr unixodbc
}
else if c(os) == "Windows" {
	local prefix "FILEPATH"
	global prefix "FILEPATH"
	local K "FILENAME:"
	global K "FILENAME:"
}

set more off
clear all
cap restore, not
cap set maxvar 20000
set seed 12345
adopath + "`prefix"FILEPATH"
adopath + "`prefix"FILEPATH"
local root "`prefix"FILEPATH"

if "`1'" == "" {
local 1 293
local 2 december_18_2017
local 3 prevalence
local 4 prev
local 5 0
local 6 0
}

local bundle_id `1'
local date `2'
local measure `3'
local short_measure `4'
local inp_only `5'
local write_dir `6'
local write_date `7'
local ms_vers `8'

quiet run "FILEPATH"

// move to submit script
//local ms_vers = "GBD2017_v6"
// local write_date = "2018_03_31"

// assign vars to identify nr data to read
if "`short_measure'" == "prev" {
	local source = "_Marketscan_prevalence"
}
if "`short_measure'" == "inc" {
	local source = "_Marketscan_incidence"
}
if `inp_only' == 0 {
	local facilities = "ALL"
}
if `inp_only' == 1 {
	local facilities = "INP_ONLY"
}

cap mkdir "$FILEPATH"
cap mkdir "$FILEPATH"
log using "$FILEPATH"

// this looks like it's just a map from age groups to gbd age_ids
// this should be fixed by using a call to shared.age_group to pull the map
//insheet using "FILEPATH", comma names clear
//	tempfile age_ids
//	save `age_ids', replace

// get a df of bundle IDs and acauses, we use this if id_to_acause fails. ID to acause is still
// kind of needed because it actually contains REI names too
quiet run "FILEPATH"
create_connection_string, server("DATABASE") database("DATABASE")
local conn_string = r(conn_string)
odbc load, exec("SQL")
	tempfile bundle_to_acause
	save `bundle_to_acause', replace

// import excel using "FILEPATH", clear firstrow
// keep if measure == "`short_measure'"
// keep if icd_ver == "ICD9_detail"
import delimited "FILEPATH", delim(",") varn(1) case(preserve) clear
	rename bundle_acause_rei acause
keep if bundle_id == `bundle_id'

// use the data from the epi db if needed
count
if r(N) < 1 {
	clear
	use `bundle_to_acause'
	keep if bundle_id == `bundle_id'
}

local acause = acause

//use "FILEPATH", clear
use "FILEPATH", clear
rename cf_final mean
rename variance standard_error
replace standard_error = sqrt(standard_error)
split acause, p("
rename acause2 bundle_id
drop acause1
destring bundle_id, replace
keep if bundle_id == `bundle_id'
keep NID location_id year age sex mean standard_error bundle_id
	rename NID nid
	rename age age_start
	rename year year_start
	tostring sex, replace
	replace sex = "Male" if sex == "1"
	replace sex = "Female" if sex == "2"
tempfile all_nr
save `all_nr', replace

if `inp_only' == 0 {
	use "FILEPATH", clear
}
if `inp_only' == 1 {
	use "FILEPATH", clear
}
if `inp_only' == 2 {
	use "FILEPATH", clear
}
if `inp_only' == 3 {
	use "FILEPATH", clear
}
if `inp_only' == 4 {
	use "FILEPATH", clear
}

// drop 2013, roll this back when it's fixed
// drop if year_start == 2013

drop mean standard_error
merge 1:1 nid location_id year_start age_start sex bundle_id using `all_nr', keep(3) nogen
//??drop me_id
// Drop data outside of age restrictons by cause
preserve
	quiet run "FILEPATH"
	create_connection_string, server("DATABASE") database(epi)
	local conn_string = r(conn_string)
	odbc load, exec("SQL")
		tempfile acauses
		save `acauses', replace
	odbc load, exec("SQL)
		tempfile meta_ids
		save `meta_ids', replace
	odbc load, exec("SQL)
		merge m:1 cause_metadata_type_id using `meta_ids', keep(3) nogen
	keep if cause_metadata_type == "yld_age_end" | cause_metadata_type == "yld_age_start"
	merge m:1 cause_id using `acauses', keep(3) nogen
	drop cause_metadata_type_id
	rename cause_metadata_value value
	reshape wide value, i(cause_id acause) j(cause_metadata_type, string)
	rename value* *
	destring yld*, replace
	keep if acause == "`acause'"
	local age_end = yld_age_end
	local age_start = yld_age_start
	if "`acause'" == "gyne_other" {
		local age_start = 15
		local age_end = 80
	}
	if "`acause'" == "imp_or_env" {
		local age_end = 80
		local age_start = 0
	}
restore
// Drop restricted ages groups
drop if age_start < `age_start'
if `age_end' != 80 {
	drop if age_start > `age_end' + 4
}

// Final formatting
	cap replace recall_type = "Period: years"
	cap replace urbanicity_type = "FILENAME"
	// cap drop design_effect uncertainty_type
	cap drop uncertainty_type_value uncertainty_type
	gen uncertainty_type_value = .
	gen uncertainty_type = "Standard error"
	replace upper = .
	replace lower = .
	drop cv_
	// gen cv_marketscan = 1

// Final correcting for noise-reduced means going over 1
	replace cases = sample_size * mean
	if "`short_measure'" == "prev" {
		replace cases = sample_size if cases > sample_size
		replace mean = 1 if mean > 1
	}

// Save US aggregate rows
preserve
	set seed 12345
	local vars_to_collapse = ""
	drop egeoloc
	foreach var of varlist * {
		if "`var'" != "location_id" & "`var'" != "location_name" & "`var'" != "cases" & "`var'" != "sample_size" & "`var'" != "mean" & "`var'" != "standard_error" {
			local vars_to_collapse = "`vars_to_collapse'" + " `var'"
		}
	}
	/*
	gen log_standard_error = (standard_error^2) / (mean^2)
	gen log_standard_deviation = log_standard_error * sqrt(sample_size)
	gen log_mean = log(mean)
	forvalues i = 0/999 {
		gen mean_`i' = rnormal(log_mean, log_standard_deviation)
		replace mean_`i' = mean * sample_size
	}
	drop mean standard_error cases
	fastcollapse mean_* sample_size, type(sum) by(`vars_to_collapse')
	forvalues i = 0/999 {
		replace mean_`i' = mean_`i' / sample_size
	}
	egen log_mean = rowmean(mean_*)
		gen mean = exp(log_mean)
	egen log_standard_deviation = rowsd(mean_*)
	gen log_standard_error = log_standard_deviation / sqrt(sample_size)
		gen standard_error = sqrt(log_standard_error / mean^2)
	gen cases = sample_size * mean
	drop mean_* log_mean log_standard_error log_standard_deviation
	*/
	fastcollapse cases sample_size, type(sum) by(`vars_to_collapse')
	gen mean = cases / sample_size
	gen standard_error = sqrt(cases) / sample_size
	gen location_id = 102
	gen location_name = "United States"
	tempfile us_aggregate
	save `us_aggregate', replace
restore
append using `us_aggregate'

// bring in claims here if inp only (singapore is only inp data)
if `inp_only' == 1 {
	* capture confirm file "FILEPATH"
	capture confirm file "FILEPATH"
	if _rc == 0 {
		di "appending Singapore and Taiwan claims data..."
		append using "FILEPATH"
		append using "FILEPATH"
		drop cv_
	}

}

// add twn inp + oup claims data
if `inp_only' == 0 {
	capture confirm file "FILEPATH"
	if _rc == 0 {
		di "appending Taiwan claims data..."
		append using "FILEPATH"
		drop cv_
	}

}

// add cols for 2016 elmo reqs
gen seq = .

// Adjust rates for maternal MEs using live birth events
//if `me_id' == 1555 | `me_id' == 1535 | `me_id' == 1543 | `me_id' == 1544 | `me_id' == 1550 | `me_id' == 3086 | `me_id' == 3085 {
if `bundle_id' == 3419 | `bundle_id' == 646 | `bundle_id' == 74 | `bundle_id' == 75 | `bundle_id' == 423 | `bundle_id' == 77 | `bundle_id' == 422 | `bundle_id' == 667 | `bundle_id' == 76 {
	preserve
		quiet run "FILEPATH"
			create_connection_string, server("DATABASE") database("DATABASE")
			local conn_string = r(conn_string)
			odbc load, exec("SQL")
			rename age_group_years_start age_start
			tempfile age_ids
			save `age_ids', replace
		//adopath + "$FILEPATH"
		//adopath + "$FILEPATH"
		quiet run "$FILEPATH"
		//quiet run "$FILEPATH"
		//get_covariate_estimates, covariate_name_short(asfr) clear
		get_covariate_estimates, covariate_id(13) sex_id("2") year_id("1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016") clear
			//replace sex_id = 2
			keep location_id year_id age_group_id sex_id mean_value
			rename year_id year_start
			merge m:1 age_group_id using `age_ids', nogen keep(3)
			rename sex_id sex
			tostring sex, replace
			replace sex = "Male" if sex == "1"
			replace sex = "Female" if sex == "2"
			tempfile asfr
			save `asfr', replace
	restore
	merge 1:1 location_id year_start age_start sex using `asfr', keep(3) nogen
	replace sample_size = sample_size * mean_value
	replace mean = cases / sample_size
}

// Covariates and save
foreach year in 2000 2010 2012 {
//foreach year in 2010 2012 {
	gen cv_marketscan_all_`year' = 0
	gen cv_marketscan_inp_`year' = 0
}

// set age end to .999 where age start is 0 per Nick JK to improve dismod models
replace age_end = 0.999 if age_start == 0 & age_end != 4
// drop US level data per Ryan
drop if location_id == 102

// do the age restrictions 1 more time to catch new claims data
// Drop restricted ages groups
drop if age_start < `age_start'
if `age_end' != 80 {
	drop if age_start > `age_end' + 4
}

drop egeoloc

// make the directory to write to, either work or test
if `write_dir' == 0 {
	// set outdir to test directory
	cap mkdir "$FILEPATH"
	cap mkdir "$FILEPATH"
	cap mkdir "$FILEPATH"
	cap mkdir "$FILEPATH"
	cap mkdir "$FILEPATH"
	local out_dir "$FILEPATH"
	cap mkdir "`out_dir'"
}

if `write_dir' == 1 {
	// set outdir to write directory
	// WRITE TO THE BUNDLE DIRECTORY FOR MODELERS
	cap mkdir "$FIELPATH"
	cap mkdir "$FILEPATH"
	cap mkdir "$FILEPATH"
	cap mkdir "$FILEPATH"
	local out_dir "$FILEPATH"
	cap mkdir "`out_dir'"
}

if `inp_only' == 1 {
	// Fix 0s
	preserve
		use "FILEPATH", clear
		sum cf_final if cf_final != 0
		local min_value = r(min)
	restore
	replace mean = 0 if mean < `min_value'
	replace cases = mean * sample_size
	//local out_dir "$FILEPATH"
	//local out_dir "$FILEPATH"
	//!mkdir "`out_dir'"
	//local out_dir "$FILEPATH"

	replace cv_marketscan_inp_2000 = 1 if year_start == 2000
	replace cv_marketscan_inp_2010 = 1 if year_start == 2010
	replace cv_marketscan_inp_2012 = 1 if year_start == 2012
	//export excel "FILEPATH", firstrow(var) sheet("extraction") replace
	order source_type
	export excel "FILEPATH", firstrow(var) sheet("extraction") replace
	//cap mkdir "$FILEPATH"
	//saveold "FILEPATH", replace
	cap mkdir "$FILEPATH"
	cap mkdir "$FILEPATH
	saveold "FILEPATH", replace
}

if `inp_only' == 0 {
	// Fix 0s
	preserve
		use "FILEPATH", clear
		sum cf_final if cf_final != 0
		local min_value = r(min)
	restore
	gen min_value = `min_value'
	replace mean = 0 if mean < min_value
	replace cases = mean * sample_size

	replace cv_marketscan_all_2000 = 1 if year_start == 2000
	replace cv_marketscan_all_2010 = 1 if year_start == 2010
	replace cv_marketscan_all_2012 = 1 if year_start == 2012
	export excel "FILEPATH", firstrow(var) sheet("extraction") replace
	cap mkdir "$FILEPATH"
	saveold "FILEPATH", replace
}

if `inp_only' == 3 {
	// Fix 0s
	preserve
		use "FILEPATH", clear
		sum cf_final if cf_final != 0
		local min_value = r(min)
	restore
	gen min_value = `min_value'
	replace mean = 0 if mean < min_value
	replace cases = mean * sample_size

	replace cv_marketscan_all_2000 = 1 if year_start == 2000
	replace cv_marketscan_all_2010 = 1 if year_start == 2010
	replace cv_marketscan_all_2012 = 1 if year_start == 2012
	export excel "FILEPATH", firstrow(var) sheet("extraction") replace
	cap mkdir "$FILEPATH"
	saveold "FILEPATH", replace
}

if `inp_only' == 4 {
	// Fix 0s
	preserve
		use "FILEPATH", clear
		sum cf_final if cf_final != 0
		local min_value = r(min)
	restore
	gen min_value = `min_value'
	replace mean = 0 if mean < min_value
	replace cases = mean * sample_size

	replace cv_marketscan_all_2000 = 1 if year_start == 2000
	replace cv_marketscan_all_2010 = 1 if year_start == 2010
	replace cv_marketscan_all_2012 = 1 if year_start == 2012
	export excel "FILEPATH", firstrow(var) sheet("extraction") replace
	cap mkdir "$FILEPATH
	saveold "FILEPATH", replace
}

if `inp_only' == 2 {
	// Fix 0s
	preserve
		use "FILEPATH", clear
		sum cf_final if cf_final != 0
		local min_value = r(min)
	restore
	replace mean = 0 if mean < `min_value'
	replace cases = mean * sample_size
	//local out_dir "$FILEPATH"
	//cap mkdir "`out_dir'"
	replace cv_marketscan_all_2000 = 1 if year_start == 2000
	replace cv_marketscan_all_2010 = 1 if year_start == 2010
	replace cv_marketscan_all_2012 = 1 if year_start == 2012
	cap mkdir "$FILEPATH"
	saveold "FILEPATH", replace
}

log close log
!rm "$FILEPATH"

// END
