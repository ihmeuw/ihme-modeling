/*
Date Created: 20 April 2012 
Description: Create an archive copy of data and/or code 
*/ 

clear all 
capture cleartmp
set more off

if (c(os)=="Unix") global root "FILEPATH"	
if (c(os)=="Windows") global root "FILEPATH"

local archive_code = 0 // 1 to archive all code
local archive_data = 1 // 1 to archive all data 

cd "FILEPATH" // master archive folder
local date: display %td_CCYY_NN_DD date(c(current_date), "DMY")
local date = subinstr(trim("`date'"), " " , "-", .)
local user = "`c(username)'"

capture mkdir "archive/`date'"
if (_rc == 693) {
	local date = "`date'_2"
	mkdir "archive/`date'"
}

if (`archive_code' == 1) { 
	local files: dir "FILEPATH" files "c*.do"
	foreach file of local files { 
		copy "FILEPATH/`file'" "archive/`date'/`file'", replace
	} 
	local files: dir "functions" files "*"
	foreach file of local files { 
		copy "functions/`file'" "archive/`date'/`file'", replace
	} 
}

if (`archive_data' == 1) { 
	local files: dir "FILEPATH/data" files "d*"
	foreach file of local files { 
		copy "FILEPATH`file'" "archive/`date'/`file'", replace
	} 
	local files: dir "FILEPATH/graphs" files "comp_*"
	foreach file of local files { 
		copy "FILEPATH/`file'" "archive/`date'/`file'", replace
	} 
}
