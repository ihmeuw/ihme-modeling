/***********************************************************************************************************
 Author:
 Date: 7/13/2015
 Project: ubCov
 Purpose: Run Script

***********************************************************************************************************/


//////////////////////////////////
// Setup
//////////////////////////////////

clear all
set more off
set obs 1


// Settings
local central_root "<<<< FILEPATH REDACTED >>>>>"
// select topic to process; can specify multiple
local topics birthhistories
cd "<<<< FILEPATH REDACTED >>>>>"

// Load functions
do "`central_root'/modules/extract/core/load.do"
// Make sure you're in central
cd `central_root'

// Initialize the system
/*
	Brings in the databases, after which you can run
	extraction or sourcing functions like: new_topic_rows

	You can view each of the loaded databases by running: get, *db* (eg. get, codebook)
*/

ubcov_path
init, topics(`topics')

local outpath "<<<< FILEPATH REDACTED >>>>>/outputs"
// list of ubcov ids to process

local array 5215

foreach number in `array'{
    local i `number'
    run_extract `i', bypass
	cap gen pweight = .
	cap gen admin_2 = .

	// renaming of vars to comply with current model reqs
	rename geospatial_id cluster_number
	rename hh_id household_number
	rename year_end year
	rename ihme_loc_id country
	rename survey_name source
	rename pweight weight
	rename survey_module survey
	gen caseid = mother_id
	rename mother_id mid
	rename child_no childs_line

	// drop module ('survey'), and rename 'source' as 'survey'
	drop survey
	gen survey = source

	if regexm(source, "/") {
	replace source = "COUNTRY_SPECIFIC"
	}

	// generate a unique survey name for output
	tostring nid, gen(temp_nid)
	tostring year, gen(temp_year)
	gen temp_id = country + " " + temp_year + " " + source + " " + temp_nid

	// create required variables
	foreach var in admin_1 admin_2 {
	capture confirm new variable `var'
	if _rc == 0 {
		gen `var' = .
		}
	}

	keep nid survey country year cluster_number household_number mothers_line caseid childs_line mothers_age mothers_age_group child_alive birthtointerview_cmc child_age_at_death_months ceb ced weight mid admin_1 admin_2 interview_date_cmc child_dob_cmc temp_id child_age_at_death_raw strata

	// reorder variables to match previous formatting
	cap order nid survey country year cluster_number household_number mothers_line caseid childs_line mothers_age mothers_age_group child_alive birthtointerview_cmc child_age_at_death_months ceb ced weight mid admin_1 admin_2 interview_date_cmc child_dob_cmc temp_id child_age_at_death_raw strata

	local filename = "<<<< FILEPATH REDACTED >>>>>/"+temp_id+".DTA"
	save "`filename'", replace emptyok

}
