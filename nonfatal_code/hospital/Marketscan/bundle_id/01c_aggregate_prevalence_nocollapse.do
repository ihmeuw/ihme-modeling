// Aggregate prevalence

if c(os) == "Unix" {
	local prefix "/home/j"
	global prefix "/home/j"
	set more off
	set odbcmgr unixodbc
}
else if c(os) == "Windows" {
	local prefix "J:"
	global prefix "J:"
}
//set tracedepth 1
//set trace on

set more off
clear all
cap restore, not
cap set maxvar 20000
set seed 12345
adopath + "`prefix'/FILEPATH"
adopath + "`prefix'/FILEPATH"
local state_map_dir "`prefix'/FILEPATH"
pause on

if "`1'" == "" {
	local 1 80
}

// Parallel by bundle_id
local bundle_id `1'

// Collapse to gbd age groups?
local gbd_age_groups = 1
local gbd_save = "_gbdages"

local temp_folder "FILEPATH"

local output_dir = "`prefix'/FILEPATH"

// TODO idk if this id_to_acause needs newest clean_map but it probs does, please check -DS
import delimited "$prefix/FILEPATH", delim(",") varn(1) case(preserve) clear
//DONE get acause from 2016
	rename bundle_acause_rei acause
	keep if bundle_id == `bundle_id'
	local acause = acause

// Load Epi extraction template
import delimited "$prefix/FILEPATH", delim(",") varn(1) case(preserve) clear
	tempfile epi_extraction_template
	save `epi_extraction_template', replace

// Load map of state ids
import delimited "`state_map_dir'/FILEPATH", delim(",") varn(1) clear
	tempfile state_ids
	save `state_ids', replace

// Load map of location_ids
quiet run "$prefix/FILEPATH"
	//create_connection
	create_connection_string, server("SERVER") database(SERVER)
	local conn_string = r(conn_string)
	odbc load, exec(SQL QUERY) `conn_string' clear
	keep if most_detailed == 1
	keep if regexm(ihme_loc_id, "USA") == 1
	drop most_detailed ihme_loc_id
	tempfile location_ids
	save `location_ids', replace

// Load map of ME names
quiet run "$prefix/FILEPATH"
	create_connection_string, server("SERVER") database("SERVER")
	local conn_string = r(conn_string)
	odbc load, exec(SQL QUERY) `conn_string' clear
	tempfile bundle_ids
	save `bundle_ids', replace

quiet run "$prefix/FILEPATH"

foreach prev_cat in all all_inp rhd_and_endo_pri_inp rhd_inp_otp{

	// Append datasets and assign NIDs
	foreach dataset in ccae2000 ccae2010 ccae2012 mdcr2000 mdcr2010 mdcr2012 {
		//quietly {
		// Load sample size to make square dataset
		local data_year = substr("`dataset'", 5, 4)
		use "FILEPATH", clear
		tempfile sample_size
		save `sample_size', replace

		clear
		tempfile `dataset'
		save ``dataset'', emptyok

		// this is inside a for loop based on data type (all, all_inp), etc so use the appropriate column from data
		local files: dir "`temp_folder'FILEPATH`prev_cat'" files "*`dataset'*.dta"
		foreach file of local files {
			append using "`temp_folder'FILEPATH`prev_cat'/`file'"
		}
		//tostring egeoloc, replace
		tostring sex, replace
		destring year, replace
		// the python script should be dropping all duplicate cases for prevalence
		// so no need to keep certain facility ids
		fastcollapse cases, type(sum) by(year egeoloc age_start age_end sex bundle_id)
		// Keep just bundle_id we are parallelizing over
		keep if bundle_id == `bundle_id'

		// Drop data outside of age restrictons by cause
		preserve
			quiet run "$prefix/FILEPATH"
			create_connection_string, server("SERVER") database("SERVER")
			local conn_string = r(conn_string)
			odbc load, exec("QUERY") `conn_string' clear
				tempfile acauses
				save `acauses', replace
			odbc load, exec("QUERY") `conn_string' clear
				tempfile meta_ids
				save `meta_ids', replace
			odbc load, exec("QUERY") `conn_string' clear
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

		gen nid = .
		replace nid = 223670 if "`dataset'" == "ccae2000"
		replace nid = 224501 if "`dataset'" == "ccae2010"
		replace nid = 224502 if "`dataset'" == "ccae2012"
		replace nid = 223672 if "`dataset'" == "mdcr2000"
		replace nid = 224499 if "`dataset'" == "mdcr2010"
		replace nid = 224500 if "`dataset'" == "mdcr2012"

		save ``dataset'', replace
		//}
		local size = _N
		di "dataset size: `size'"
} // end of the "foreach dataset" loop

// Collapse over source to a new NID "marketscan commericial claims and medicare" because our denominator is "everyone fully covered in the year by EITHER commercial insurance or medicare". Merge back on sample size after collapsing cases.
// Append collapsed datasets
clear
foreach dataset in ccae2000 ccae2010 ccae2012 mdcr2000 mdcr2010 mdcr2012 {
		append using ``dataset''
	}

replace bundle_id = `bundle_id'
fastcollapse cases, type(sum) by(sex year egeoloc age_start age_end bundle_id)
tempfile all_data
save `all_data', replace

// Append sample size years
clear
tempfile all_sample_size
save `all_sample_size', emptyok
foreach year in 2000 2010 2012 {
//foreach year in 2010 2012 {
	use "FILEPATH", clear
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
// placeholder NID
gen nid = 244369 if year == 2000
replace nid = 244370 if year == 2010
replace nid = 244371 if year == 2012

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
gen age_demographer = 0
gen unit_type = "Person"
gen unit_value_as_published = 1
gen measure_issue = 0
gen measure_adjustment = 0
gen extractor = "USER and USER"
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
import delimited "$prefix/FILEPATH", delim(",") varn(1) case(preserve) clear
	rename bundle_acause_rei acause
	keep bundle_id acause
	keep if bundle_id == `bundle_id'
	local acause = acause
	tempfile acauses
	save `acauses', replace
use `epi_extraction_template', clear
	foreach var of local vars_we_have {
		cap drop `var'
	}
	append using `formatted'
merge m:1 bundle_id using `acauses', keep(3) nogen

// Save to modelers folders by ME id
local acause = acause
drop acause
replace is_outlier = 0
order nid location_id source_type year_start year_end sex age_start age_end cases sample_size location_name bundle_id bundle_name measure mean standard_error
sort nid location_id source_type year_start year_end sex age_start age_end

// Format for noise reduction
// Save copy with info for epi upload
drop if age_start == 64 // values high only in this year for pretty much all causes, we think it's double-counting as people switch coverage status

cap mkdir "FILEPATH"
cap mkdir "FILEPATH"
cap mkdir "FILEPATH"
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
gen acause = "`acause'#`bundle_id'"
rename nid NID
rename age_start age
rename year_start year
replace sex = "1" if sex == "Male"
replace sex = "2" if sex == "Female"
destring sex, replace
order acause NID location_id iso3 list national region source source_label source_type subdiv year age sex sample_size cf_raw cf_corr cf_rd cf_final
cap mkdir "$prefix/FILEPATH"
cap mkdir "$prefix/FILEPATH"
save "$prefix/FILEPATH", replace

}

log close log
// END
