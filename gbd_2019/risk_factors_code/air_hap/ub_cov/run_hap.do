/***********************************************************************************************************
 Project: ubCov
 Purpose: Extract Script air_hap

 FILEPATH
***********************************************************************************************************/


//////////////////////////////////
// Setup
//////////////////////////////////

if c(os) == "Unix" {
    local j "ADDRESS"
	local h "ADDRESS'"
	local l "ADDRESS"
    set odbcmgr unixodbc
	local central_root "FILEPATH"
}
else if c(os) == "Windows" {
    local j "ADDRESS:"
	local h "ADDRESS:"
	local l "ADDRESS:"
	local central_root "FILEPATH"
}

clear all
set more off
set obs 1

///////////////////////////////////////////////////////////////////////
/*  Arguments:
		- topic: your research topic
        - ubcov_id: The id of the codebook row
		- outpath_L: output file path for limited drive files
		- outpath_J: output file path for general files
    Optional:
        - keep: Keeps both raw data and extracted data, allows manual extraction check before final output
        - bypass: Skips the extraction check, output the data 
        - run_all: Loops through all ubcov_ids in the codebook.
*/
////////////////////////////////////////////////////////////////////////

local topics hap
local array // enter ubcov id of survey(s) to be extracted

local outpath_L "FILEPATH"
local outpath_J "FILEPATH"
local options //bypass //leave it blank if you don't want to keep, bypass, or run_all. If you are running on cluster, you have to use bypass as an option otherwise it will error.



///////////////////////////////////////////////////////////////////////
//Extraction
//////////////////////////////////////////////////////////////////////

// Load functions
cd "`central_root'"
do "FILEPATH/load.do"

// Load the base code for ubCov
local paths  `central_root'/modules/extract/core/ `central_root'/modules/extract/core/addons/
foreach path in `paths' {
    local files : dir "`path'" files "*.do"
    foreach file in `files' {
        if "`file'" != "run.do" do "`path'/`file'"
    }
}

// Make sure you're in central
cd `central_root'

// Initialize the system
ubcov_path
init, topics(`topics')

// Launches extract
foreach number in `array'{
    local uid `number'
    run_extract `uid', `options'
    tostring year_start, gen(year)
    tostring year_end, gen(end_year)
    tostring nid, gen(nid_n)
    local filename = survey_name + "_" + nid_n + "_" + survey_module + "_" + ihme_loc_id + "_" + year + "_" + end_year
    local filename = subinstr("`filename'", "/", "_",.)
	
    drop year end_year nid_n
	if (strpos("$file_path", "FILEPATH")|strpos("$file_path", "FILEPATH")){
		local outpath = "`outpath_L'"
	}
	else{
		local outpath = "`outpath_J'"
	}
	cd  `outpath'
    export delim "`filename'", replace
}

