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
local central_root "<<<< FILEPATH REDACTED >>>>>"														// This is set to the folder which contains the ubcov code
global outpath "<<<< FILEPATH REDACTED >>>>>"											// Set this as the folder you wish to save your extracts in
local geographies "<<<< FILEPATH REDACTED >>>>>\combined_codebook.dta"								//Set this filepath so be that where you have a combined geography codebook dta file saved
local topics sbh																									// List of this topics you want to extract data for
local array 189        																								// List the UbCov ID's you wish to extract here

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
	rename geospatial_id cluster_number
	rename year_end year
	gen country = substr(ihme_loc_id,1,3)
	rename ihme_loc_id iso3
	rename survey_name source
	cap gen pweight = .
	rename pweight weight
	rename survey_module survey
	tostring year, gen(temp_year)
	if regexm(source, "/") {
	replace source = "COUNTRY_SPECIFIC"
	}
	tostring nid, gen (temp_nid)
	gen temp_id = iso3 + " " + temp_year + " " + survey + " " + source + " " + temp_nid

	//format variabels to %20.0f
	tostring cluster_number, replace format("%20.0f")
	cap tostring line_id, replace format("%20.0f")
	cap tostring hh_id, replace format("%20.0f")


	merge m:1 nid iso3 cluster_number using "`geographies'"

	sum _merge
	if r(min) == 1{
	noisily: di as error "Not all clusters match to geography codebook"													// if there are records which dont match throw up an error to check it
	STOP
	}
	keep if _merge ==3
	global filename = "$outpath"+"WN/"+temp_id+".DTA"

	// if age_year isnt in the dataset (i.e only categories available) gen a missing variable
	cap gen age_year = .

	//keep and order variables
	keep nid country year source survey strata cluster_number hh_id line_id weight age_year age_group_of_woman ceb ced point latnum longnum location_name location_code admin_level shapefile
	order nid country year source survey strata cluster_number hh_id line_id weight age_year age_group_of_woman ceb ced point latnum longnum location_name location_code admin_level shapefile

	save "$filename", replace emptyok

	/////////////////////////////////////
	//Aggregate to one row per location//
	/////////////////////////////////////

	drop if location_name == ""
	// create a temp file with ced and mean age for each location
	collapse (sum) ced (mean)age_year,by(nid source country year location_name survey point latnum longnum location_code admin_level shapefile)
	tempfile temp1
	save "`temp1'"

	use "$filename"
	gen number_of_women = 1
	collapse (sum) ceb number_of_women, by(nid source country year location_name survey age_group_of_woman point latnum longnum location_code admin_level shapefile)

	reshape wide number_of_women ceb , i(country point latnum longnum location_name location_code admin_level shapefile) j(age_group_of_woman)
	//calculate total ceb and ced and the proportions for each agegroup
	//IFor surveys which do not have allage groups of women create the ceb and number of women for this age group
	if "$nid" == "23219"|"$nid" == "23165" | "$nid" == "8750" {
		gen number_of_women7 = 0
		gen ceb7 = 0
		}
	egen ceb = rowtotal(ceb1 ceb2 ceb3 ceb4 ceb5 ceb6 ceb7)
	egen sum_women = rowtotal(number_of_women1 number_of_women2 number_of_women3 number_of_women4 number_of_women5 number_of_women6 number_of_women7)
	gen women_15_19	= number_of_women1/sum_women
	gen women_20_24	= number_of_women2/sum_women
	gen women_25_29	= number_of_women3/sum_women
	gen women_30_34	= number_of_women4/sum_women
	gen women_35_39	= number_of_women5/sum_women
	gen women_40_44	= number_of_women6/sum_women
	gen women_45_49	= number_of_women7/sum_women
	gen ceb_15_19 = ceb1/ceb
	gen ceb_20_24 = ceb2/ceb
	gen ceb_25_29 = ceb3/ceb
	gen ceb_30_34 = ceb4/ceb
	gen ceb_35_39 = ceb5/ceb
	gen ceb_40_44 = ceb6/ceb
	gen ceb_45_49 = ceb7/ceb

	drop ceb1 number_of_women1 ceb2 number_of_women2 ceb3 number_of_women3 ceb4 number_of_women4 ceb5 number_of_women5 ceb6 number_of_women6 ceb7 number_of_women7

	merge 1:1 country point latnum longnum location_name location_code admin_level shapefile nid source year survey using "`temp1'"

	drop _merge
	order country year location_code shapefile admin_level point latnum longnum ced sum_women ceb women_15_19 women_20_24 women_25_29 women_30_34 women_35_39 women_40_44 women_45_49 ceb_15_19 ceb_20_24 ceb_25_29 ceb_30_34 ceb_35_39	ceb_40_44 ceb_45_49 source survey nid location_name age_year
	rename age_year mean_matage
	local file_out = subinstr("$filename", "WN", "Aggregated", 1)
	save "`file_out'", emptyok replace
}

////////////////////////////
// Merge datasets together//
/////////////////////////////

// 1 WN files
// Due to the size and number of WN files and limited computing power, after running the extraction I have divided them into folder named WN1-10
// with the files split reasonably equally between these to allow timely computing.

// Create a file in each directory called "mergeonme.DTA", this must have all of the
// required variables (can be a copy of another file) in it and replace the NID with 989898
// All datasets are appended onto this and then these records deleted. (I couldnt find a way of
// mergeing the datasets without duplicating the file it is merged onto - anyone feel free to fix this!_

set more off
local array 1 2 3 4 5 6 7 8 9 10
foreach number in `array' {
local i `number'
cd "<<<< FILEPATH REDACTED >>>>>/WN`i'"
use "mergeonme.DTA", clear
local files : dir ""files "*.DTA"

foreach file in `files' {
	append using "`file'"
}
drop if nid == 989898
export delimited using "Combined_SBH_WN`i'", replace
}


// 2 Aggregated files
// Create a file in the directory called "mergeonme.DTA", this must have all of the
// required variables (can be a copy of another file) in it and replace the NID with 989898
// All datasets are appended onto this and then these records deleted. (I couldnt find a way of
// mergeing the datasets without duplicating the file it is merged onto - anyone feel free to fix this!_

set more off
cd "<<<< FILEPATH REDACTED >>>>>/Aggregated/"
local files : dir ""files "*.DTA"
use "mergeonme.DTA", clear
local files : dir ""files "*.DTA"
foreach file in `files' {
	append using "`file'"
}

drop if nid == 989898
export delimited using "Combined_SBH_Aggregated", replace
