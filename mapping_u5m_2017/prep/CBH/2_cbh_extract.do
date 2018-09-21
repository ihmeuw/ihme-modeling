/***********************************************************************************************************
 Author: 
 Project: ubCov
 Purpose: CBH Run Script
***********************************************************************************************************/


//////////////////////////////////
// Setup
//////////////////////////////////

clear all
set more off
set obs 1

// Set locals
local central_root "<<<< FILEPATH REDACTED >>>>>"				// This is set to the folder which contains the ubcov code
local topics cbh															// List of this topics you want to extract data for
global outpath "<<<< FILEPATH REDACTED >>>>>"	// Set this as the folder you wish to save your extracts in.
local array 4664        													// List the UbCov ID's you wish to extract here

// Set working directory
cd `central_root'

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

// Run the extraction

foreach number in `array'{
    local i `number'
    run_extract `i', bypass bypass_map
	gen country = substr(ihme_loc_id,1,3)
	rename year_end year
	rename ihme_loc_id iso3
	rename survey_name source
	cap gen pweight = .
	rename pweight weight
	rename survey_module survey
	tostring year, gen(temp_year)
	if regexm(source, "/") {
	replace source = "COUNTRY_SPECIFIC"
	}
	tostring nid, gen(temp_nid)
	gen temp_id = iso3 + "_" + temp_year + "_" + source + "_" + survey +"_"+ temp_nid

	cap tostring geospatial_id, replace
	rename geospatial_id cluster_number

	merge m:1 nid iso3 cluster_number using "<<<< FILEPATH REDACTED >>>>>\combined_codebook.dta"		//Set this filepath so be that where you have a combined geography codebook dta file saved
	// if there are records which dont match throw up an error to check it
	sum _merge
	if r(min) == 1{
	noisily: di as error "Not all clusters match to geography codebook"
	STOP
	}

	keep if _merge == 3

	local filename = "$outpath"+temp_id+".DTA"


	// Drop unwanted variables in datathe variables you want to keep in the dataset
	foreach var in psu urban file_path hhweight smaller_site_unit year_start temp_year temp_nid admin_1_mapped admin_1_id iso3 urban age_day file_path year_start temp_year temp_nid _merge temp_id child_age_at_death_raw admin_1 admin_2 admin_3 admin_4 buffer uncertain_point iso3{
		cap drop `var'
	}

	save "`filename'", replace emptyok

}

////////////////////////////
// Merge datasets together//
/////////////////////////////
// Create a file in the directory called "mergeonme.DTA", this must have all of the
// required variables (can be a copy of another file) in it and replace the NID with 989898
// All datasets are appended onto this and then these records deleted. (I couldnt find a way of
// mergeing the datasets without duplicating the file it is merged onto - anyone feel free to fix this!_

set more off
cd "$outpath"
use "mergeonme.DTA", clear
local files : dir ""files "*.DTA"

foreach file in `files' {
		append using "`file'", force
}

drop if nid == 989898
export delimited using "Combined_CBH", replace
