// Aggregate prevalence

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
	local K "FILENAME"
	global K "FILENAME"
}
//set tracedepth 1
//set trace on

set more off
clear all
cap restore, not
cap set maxvar 20000
set seed 12345
adopath + "`prefix"FILEPATH"
adopath + "`prefix"FILEPATH"
local state_map_dir "`prefix"FILEPATH"
pause on

if "`1'" == "" {
	local 1 131
	local 2 = "test_3"
}

// Parallel by bundle_id
local bundle_id `1'
global run_id "`2'"

// Collapse to gbd age groups?
local gbd_age_groups = 1
local gbd_save = "_gbdages"

//local temp_folder "FILENAME" //this is where the data is read from
local temp_folder "FILEPATH"

// sorta strange, but this creates a log and deletes it at the end of the script if everything is good
// local log_dir = "`prefix"FILEPATH"
//cap mkdir "`log_dir"FILEPATH"
// cap mkdir "`log_dir'"
// log using "`log_dir'/aggregate_all_prevalence_`bundle_id'.smcl", replace name(log)

// read in id_to_acause file to get REI names for bundles
// acause from the db will be used when possible, this is an older method and mainly gets dummy acauses for REIs
//import delimited "FILEPATH", delim(",") varn(1) case(preserve) clear
import delimited "FILEPATH", delim(",") varn(1) case(preserve) clear
	rename bundle_acause_rei acause
	keep bundle_id acause
	keep if bundle_id == `bundle_id'
	tempfile bundle_to_acause
	save `bundle_to_acause', replace

// get acause name for bundles
	keep if bundle_id == `bundle_id'
	drop if acause == ""
	// local acause = acause

	// use the data from the id to acause file if needed
	count
	if r(N) < 1 {
		clear
		use `bundle_to_acause'
		keep if bundle_id == `bundle_id'
	}
	local acause = acause
	tempfile acauses
	save `acauses', replace

// Load Epi extraction template
import delimited "FILEPATH", delim(",") varn(1) case(preserve) clear
	tempfile epi_extraction_template
	save `epi_extraction_template', replace

// Load map of state ids
import delimited "FILEPATH", delim(",") varn(1) clear
	tempfile state_ids
	save `state_ids', replace

// Load map of location_ids
use "FILEPATH", replace
	tempfile location_ids
	save `location_ids', replace

// Load map of ME names
	use "FILEPATH", replace
	tempfile bundle_ids
	save `bundle_ids', replace

quiet run "FILEPATH"

foreach prev_cat in all all_inp {

	clear
	// read in all the data
	local files: dir "FILEPATH"
	foreach file of local files {
		append using "FILEPATH"
		// Keep just bundle_id we are parallelizing over
		keep if bundle_id == `bundle_id'
	}

	// Keep just bundle_id we are parallelizing over
	keep if bundle_id == `bundle_id'
	// merge on egeoloc

	tempfile all_years
	save `all_years', replace

	// Append datasets and assign NIDs
	foreach data_year in 2000 2010 2011 2012 2013 2014 2015 2016 {
		// to read in sample size by year
		use "FILEPATH", clear
		tempfile sample_size
		save `sample_size', replace

		clear
		tempfile `data_year'
		save ``data_year'', emptyok

		use `all_years', clear

		//tostring egeoloc, replace
		tostring sex, replace
		destring year, replace

		// drop rows not in the year
		keep if year == `data_year'

		local size = _N
		if `size' < 1 {
			di "dataset size: `size'"
			continue
		}

		// the python script should be dropping all duplicate cases for prevalence
		// so no need to keep certain facility ids
		fastcollapse cases, type(sum) by(year egeoloc age_start age_end sex bundle_id)
		// Keep just bundle_id we are parallelizing over
		keep if bundle_id == `bundle_id'
		// FLAG won't include this in the python script output
		//drop sample_size

		// Drop data outside of age restrictons by cause
		// TODO pull this up a level so that it doesn't have to be ran every time
		preserve
			quiet run "FILEPATH"
			create_connection_string, server("DATABASE") database("DATABASE")
			local conn_string = r(conn_string)
			odbc load, exec("DB QUERY")
				tempfile acauses
				save `acauses', replace
			odbc load, exec("DB QUERY")
				tempfile meta_ids
				save `meta_ids', replace
			odbc load, exec("DB QUERY")
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
		drop if cases == .

		// Aggregate to GBD age groups
		if _N != 0 { // Some sources actually have 0 cases, so it'll break below. None are going to merge anyway.
			if `gbd_age_groups' == 1 {
				rename age_start age
				drop age_end
				replace age = 100 if age > 100
				gen age_start = .
				forvalues i = 4(5)94 {
					replace age_start = `i' + 1 if age > `i'
				}
				replace age_start = 0 if age == 0
				replace age_start = 1 if age < 5 & age_start == .
				gen age_end = age_start + 4
				replace age_end = 100 if age_end == 99
				replace age_end = 4 if age_start == 1
				replace age_end = 1 if age_start == 0
				fastcollapse cases, type(sum) by(sex egeoloc age_start age_end bundle_id year)
			}
			if `gbd_age_groups' != 1 {
				replace age_start = 95 if age_start >= 95
				replace age_end = 100 if age_end >= 95
				fastcollapse cases, type(sum) by(sex egeoloc age_start age_end bundle_id year)
			}
		}
		tempfile to_merge
		save `to_merge', replace

		// Merge to square dataset of sample sizes so we count true 0s
		use `sample_size', clear
		if `gbd_age_groups' == 1 {
			rename age_start age
			drop age_end
			replace age = 100 if age > 100
			gen age_start = .
			forvalues i = 4(5)94 {
				replace age_start = `i' + 1 if age > `i'
			}
			replace age_start = 0 if age == 0
			replace age_start = 1 if age < 5 & age_start == .
			gen age_end = age_start + 4
			replace age_end = 100 if age_end == 99
			replace age_end = 4 if age_start == 1
			replace age_end = 1 if age_start == 0
			fastcollapse sample_size, type(sum) by(sex egeoloc age_start age_end year)
		}
		if `gbd_age_groups' != 1 {
			replace age_start = 95 if age_start >= 95
			replace age_end = 100 if age_end >= 95
			fastcollapse sample_size, type(sum) by(sex egeoloc age_start age_end year)
		}
		destring egeoloc, replace
		save `sample_size', replace
		merge 1:1 sex age_start age_end year egeoloc using `to_merge', nogen
		replace cases = 0 if cases == .
		replace cases = sample_size if cases > sample_size

		// Drop restricted ages groups
		drop if age_start < `age_start'
		if `age_end' != 80 {
			drop if age_start > `age_end' + 4
		}


		save ``data_year'', replace
		//}
		local size = _N
		di "dataset size: `size'"
} // end of the "foreach dataset" loop

// Append collapsed datasets
clear
foreach data_year in 2000 2010 2011 2012 2013 2014 2015 2016 {
		append using ``data_year''
	}

replace bundle_id = `bundle_id'
fastcollapse cases, type(sum) by(sex year egeoloc age_start age_end bundle_id)
tempfile all_data
save `all_data', replace

// Append sample size years
clear
tempfile all_sample_size
save `all_sample_size', emptyok

foreach year in 2000 2010 2011 2012 2013 2014 2015 2016 {

	// FLAG TODO update this as well. why is this happening again
	use "FILEPATH", clear
	// old use "FILEPATH", clear
	destring egeoloc, replace
	if `gbd_age_groups' == 1 {
		rename age_start age
		drop age_end
		replace age = 100 if age > 100
		gen age_start = .
		forvalues i = 4(5)94 {
			replace age_start = `i' + 1 if age > `i'
		}
		replace age_start = 0 if age == 0
		replace age_start = 1 if age < 5 & age_start == .
		gen age_end = age_start + 4
		replace age_end = 100 if age_end == 99
		replace age_end = 4 if age_start == 1
		replace age_end = 1 if age_start == 0
		fastcollapse sample_size, type(sum) by(sex egeoloc age_start age_end year)
	}
	if `gbd_age_groups' != 1 {
		replace age_start = 95 if age_start >= 95
		replace age_end = 100 if age_end >= 95
		fastcollapse sample_size, type(sum) by(sex egeoloc age_start age_end year)
	}
	append using `all_sample_size'
	save `all_sample_size', replace
}
// Merge data on to square sample size by all years
merge 1:1 sex age_start age_end year egeoloc using `all_data', assert(1 3) keep(1 3) nogen

// FLAG TODO UPDATE MERGED NIDs
// placeholder NID
gen nid = 244369 if year == 2000
replace nid = 244370 if year == 2010
replace nid = 244371 if year == 2012
replace nid = 336850 if year == 2011
replace nid = 336849 if year == 2013
replace nid = 336848 if year == 2014
replace nid = 336847 if year == 2015
replace nid = 408680 if year == 2016

di "Formatting for epi template..."

// Format for Epi
//destring egeoloc, replace force
merge m:1 egeoloc using `state_ids', keep(3) nogen
rename state location_name
replace location_name = substr(location_name, 1, length(location_name) - 1)
replace location_name = "District of Columbia" if location_name == "Washington DC"
merge m:1 location_name using `location_ids', keep(3) nogen
gen source_type = "Facility - other/unknown"
tostring sex, replace
	replace sex = "Male" if sex == "1"
	replace sex = "Female" if sex == "2"
rename year year_start
gen year_end = year_start
//gen bundle_id = `bundle_id'
merge m:1 bundle_id using `bundle_ids', keep(3) nogen
gen representative_name = "Nationally and subnationally representative"
gen measure = "prevalence"
gen mean = cases / sample_size
gen standard_error = sqrt(cases) / sample_size if cases > 5
	replace standard_error = (((5 - cases) / sample_size) + cases * sqrt(5/sample_size^2)) / 5 if cases <= 5
gen year_issue = 0
gen sex_issue = 0
gen age_issue = 0
gen age_demographer = 1
gen unit_type = "Person"
gen unit_value_as_published = 1
gen measure_issue = 0
gen measure_adjustment = 0
gen extractor = "clinical_team"
gen uncertainty_type = "Sample size"
gen urbanicity_type = "Unknown"
gen recall_type = "Not Set"

// Append Epi template to fill in missing columns
local vars_we_have = ""
foreach var of varlist * {
	local vars_we_have `vars_we_have' `var'
}
tempfile formatted
save `formatted', replace

use `epi_extraction_template', clear
	foreach var of local vars_we_have {
		cap drop `var'
	}
	append using `formatted'

replace is_outlier = 0
order nid location_id source_type year_start year_end sex age_start age_end cases sample_size location_name bundle_id bundle_name measure mean standard_error
sort nid location_id source_type year_start year_end sex age_start age_end

// Format for noise reduction
// Save copy with info for epi upload
drop if age_start == 64 // Crazy high only in this year for pretty much all causes, we think it's double-counting as people switch coverage status

//cap mkdir "FILENAME"
//cap mkdir "FILENAME"
cap mkdir "FILEPATH"

// I'm not sure exactly what this gets used for
save "FILEPATH", replace
// Save copy formatted for noise reduction code
keep nid location_id year_start age_start sex sample_size mean
foreach var in cf_raw cf_corr cf_rd cf_final {
	gen `var' = mean
}
drop mean
gen iso3 = "USA"
gen list = "ICD9_detail"
gen national = 1
gen region = 100 // location_id for region USA is in
gen source = "_Marketscan_prevalence"
gen source_label = source
gen source_type = "Marketscan"
gen subdiv = ""
gen acause = "`acause'
rename nid NID
rename age_start age
rename year_start year
replace sex = "1" if sex == "Male"
replace sex = "2" if sex == "Female"
destring sex, replace
order acause NID location_id iso3 list national region source source_label source_type subdiv year age sex sample_size cf_raw cf_corr cf_rd cf_final

// These files are fed into the NR process
cap mkdir "FILEPATH"

save "FILEPATH", replace

}
