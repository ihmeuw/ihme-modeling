/***********************************************************************************************************
 Project: ubCov
 Purpose: Run Script
 Run: do FILEPATH																					
***********************************************************************************************************/


//////////////////////////////////
// Setup
//////////////////////////////////

if c(os) == "Unix" {
    local j "ADDRESS"
    local h "ADDRESS"
    local l "ADDRESS"
    set odbcmgr unixodbc
}
else if c(os) == "Windows" {
    local j "ADDRESS"
    local h "ADDRESS"
    local l "ADDRESS"
}
clear all
set maxvar 10000
set more off
set obs 1

// Settings
local central_root "FILEPATH"
local topics wash

// Load the base code for ubCov
cd "ADDRESS"
do "FILEPATH"

// Initialize the system
/* 
	Brings in the databases, after which you can run
	extraction or sourcing functions like: new_topic_rows

	You can view each of the loaded databases by running: get, *db* (eg. get, codebook)
*/

ubcov_path
init, topics(`topics')

// Run extraction
/* Launches extract

	Arguments:
		- ubcov_id: The id of the codebook row
	Optional:
		- keep: Keeps 
		- bypass: Skips the extraction check
		- run_all: Loops through all ubcov_ids in the codebook.
*/

//Versioning
local run "run1" //limited to most recent (post 2010 surveys) sources to add important ones for review week
local run "run2" //batch extract with geospatial strings
local run "run3" //revisions to custom code
local run "run4" //more strings mapped correctly
local run "run5" //and more strings mapped correctly
local run "run6" //just extract water treatment vars
local run "run7" //documentation
local run "run8" //same as above
local run "run9" //testing 050719
local run "run10" //misc extractions gbd 2019

//Set out directory
local outpath = "FILEPATH"
cap mkdir "FILEPATH"
cap mkdir "FILEPATH"
cap mkdir "FILEPATH"
cap mkdir "FILEPATH"

local stata_shell 		"FILEPATH"
local logs 				FILEPATH

local array // Enter UbCov IDs here

//Loop through each source and extract
//foreach id of local i {
foreach id in `array' {
	run_extract `id', bypass store_vals 
	// do "FILEPATH"
	do "FILEPATH"
	// do "FILEPATH"
	tostring year_start, gen(year_n)
	tostring year_end, gen(end_year_n)
	tostring nid, gen(nid_n)
	local filename = survey_name + "_" + nid_n + "_" + survey_module + "_" + ihme_loc_id + "_" + year_n + "_" + end_year_n
	local filename = subinstr("`filename'", "/", "_",.)
	drop year_n end_year_n nid_n
	memory
	if r(data_data_u)>5.5e+08{
		save "`outpath'/census/`filename'", replace
	}
	else{
		save "`outpath'/survey/`filename'", replace
	}

	clear
	set obs 1
	gen ubcov_id = `id'
	save "`outpath'/logs/`filename'", replace
}
