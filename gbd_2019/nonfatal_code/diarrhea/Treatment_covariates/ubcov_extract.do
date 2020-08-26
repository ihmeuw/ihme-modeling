//----HEADER-------------------------------------------------------------------------------------------------------------
// Date:    Sept 2017
// Purpose: Extract
//-----------------------------------------------------------------------------------------------------------------------


//----PREP---------------------------------------------------------------------------------------------------------------
clear

if c(os) == "Unix" {
	local j "ADDRESS"
	local h "ADDRESS"
	set odbcmgr unixodbc
} 
else if c(os) == "Windows" {
	local j "ADDRESS"
	local h "ADDRESS"
} 
else if c(os) == "Darwin" {
	local j "ADDRESS"
	local h "ADDRESS"
}

clear all
set more off
set obs 1

// Settings
local central_root "ADDRESS"
local output_root "ADDRESS"
display "`output_root'"
local topics diarrhea //lri OR diarrhea

// Load functions
cd "ADDRESS""
do "FILEPATH"


// Topic clean
local main_topic = word("`topics'", 1)
cd ADDRESS
ubcov_path
init, topics(`topics')

// Clear folders
//!rm -rf "ADDRESS"
//!mkdir "ADDRESS"
//cap mkdir "ADDRESS"
//-----------------------------------------------------------------------------------------------------------------------


//----RUN----------------------------------------------------------------------------------------------------------------
local ubcov_ids //UBCOV IDS HERE

//needs interaction: 
foreach number in `ubcov_ids' {
    local i `number'
    run_extract `i', bypass keep  
    local survey_name = "$survey_name"
    local survey_name = subinstr("`survey_name'", "/", "_", .)
	export delimited "FILEPATH", replace
}


//batch_extract, topics(`topics') ubcov_ids(`ubcov_ids') /// 
//				central_root(`central_root') ///
//				cluster_project(ihme_general) ///
//				output_path("`output_root'/`main_topic'") ///
//				store_vals_path("ADDRESS") logs_path("ADDRESS") ///
//				run_log_path("ADDRESS") ///
//				db_path("ADDRESS")
-----------------------------------------------------------------------------------------------------------------------
